FROM python:3.9-slim

# System dependencies
RUN apt-get update && apt-get install -y \
    chromium-driver \
    chromium \
    fonts-liberation \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Nastavi environment za Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV PATH="${PATH}:/usr/bin/chromium"

# Nastavi delovno mapo
WORKDIR /app

# Kopiraj datoteke
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bloker_gcs.py .

# Zagon kot privzeti ukaz
CMD ["python", "bloker_gcs.py"]
