from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
import os
import time
from datetime import datetime, timezone

# TODO popravit to v .env file
EMAIL = "dominik.ursic@dodonaanalytics.com"


EXPORT_DIR = "/tmp/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

#browser nastavitve
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=3840,2160") # da so vsi elementi vidni

prefs = {
    "download.default_directory": EXPORT_DIR,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

# blocker za vse reqeuste ki bi naložili zemljevid
driver.execute_cdp_cmd("Network.enable", {})
driver.execute_cdp_cmd("Network.setBlockedURLs", {
    "urls": [
        "*mapbox.com/*",
        "*tiles.mapbox.com/*",
        "*tile.openstreetmap.org/*",
        "*tilelayer*",
        "*VectorTile*",
        "*features*",
        "*geojson*"
    ]
})

# GCS
def upload_to_gcs(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Preneseno v GCS: gs://{bucket_name}/{destination_blob}")

try:
    
    driver.get("https://www.nationalgrid.co.uk/network-opportunity-map/")
    time.sleep(5)

    try:
        cookie_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, 'a[title="Accept all optional cookies"]')))
        cookie_btn.click()
    except:
        print("Err")

    login_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href^="/customer-portal/login"]')))
    driver.execute_script("arguments[0].click();", login_btn)
    time.sleep(3)

    email_input = wait.until(EC.visibility_of_element_located((By.ID, "customer-portal-form-field__emailAddress")))
    password_input = wait.until(EC.visibility_of_element_located((By.ID, "customer-portal-form-field__password")))

    # Vnesi podatke
    email_input.clear()
    email_input.send_keys(EMAIL)
    password_input.clear()
    password_input.send_keys(PASSWORD)
    print("Podatki vnešeni")

    login_button = wait.until(EC.element_to_be_clickable((
        By.CSS_SELECTOR,
        "button.button.button--primary.button--contextual.customer-portal-form__button"
    )))
    driver.execute_script("arguments[0].scrollIntoView(true);", login_button)
    time.sleep(1)
    driver.execute_script("arguments[0].click();", login_button)

    time.sleep(10)
    current_url = driver.current_url
    print("Trenutni URL:", current_url)

    if "login" in current_url or "error" in current_url.lower():
        raise Exception("Prijava neuspešna")

    print("Prijava uspešna!")
    # reddirect 
    driver.get("https://www.nationalgrid.co.uk/our-network/network-capacity-map-application")
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(5)

    try:
        data_button = wait.until(EC.element_to_be_clickable((By.ID, "data-pill")))
        driver.execute_script("arguments[0].click();", data_button)
    except:
        print("Ne najdem Data")

    try:
        consent_box = wait.until(EC.element_to_be_clickable((By.ID, "consent")))
        driver.execute_script("arguments[0].scrollIntoView(true);", consent_box)
        consent_box.click()

        ok_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.btn--primary.btn--continue")))
        driver.execute_script("arguments[0].click();", ok_btn)
        time.sleep(2)
    except:
        print("err")

    try:
        open_sidebar_btn = wait.until(EC.element_to_be_clickable((
            By.CSS_SELECTOR,
            "button.btn.btn--continue.btn--default.btn--small"
        )))
        open_sidebar_btn.click()
        time.sleep(2)
    except:
        print("Ni levga dela")

    try:
        data_label = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//label[contains(text(), 'Data')]")))
        driver.execute_script("arguments[0].scrollIntoView(true);", data_label)
        data_label.click()
    except:
        print("Ni Data")

    time.sleep(5)
    WebDriverWait(driver, 30).until(
        lambda d: d.find_elements(By.CSS_SELECTOR, "div.loading-shadow[aria-hidden='false']") == []
    )

    export_button = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "button.btn.btn--primary.export-button")))
    driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", export_button)

    WebDriverWait(driver, 30).until_not(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.btn--primary.export-button.btn--loading")
        )
    )

    time.sleep(3)  # počakaj na prenos
    exported_files = [f for f in os.listdir(EXPORT_DIR) if f.endswith(".csv")]
    if not exported_files:
        raise Exception("Ni datoteke za upload")

    latest_file = max(
        exported_files,
        key=lambda f: os.path.getctime(os.path.join(EXPORT_DIR, f))
    )
    full_path = os.path.join(EXPORT_DIR, latest_file)

    # Upload v GCS
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    upload_to_gcs(
        bucket_name="diplomska-461311_cloudbuild",
        source_file=full_path,
        destination_blob="exports/wpd_network_capacity_map_{}.csv".format(timestamp)
    )


except Exception as e:
    print("Napaka:", repr(e))
    driver.save_screenshot("/tmp/error.png")

finally:
    driver.quit()
