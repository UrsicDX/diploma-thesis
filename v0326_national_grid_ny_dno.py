import pandas as pd
from sqlalchemy.engine.base import Connection
from db_version.abstract_script_runner import AbstractScriptRunner
import os
import datetime as dt

from config import log, gc_client, data_dir
from nodeps.utils.generic_dataset import gdutil
from utils.dcl.arcgis_rest_api.data_lake_reader import ArcGISRestApiDLReader
from utils.dcl.dno_general import (
    generate_label,
    add_voltage_bucket,
    add_us_voltage_bucket,
    add_voltage_code_iec_60038,
)
from utils.dcl.helpers import concat_gdf

DATASET_SOURCE_PARAMETERS = {
    "delete_at_start": True,
    "name": f"national_grid_ny",
    "description": "National Grid NY",
    "url": "https://systemdataportal.nationalgrid.com/NY/",
    "access_type": "PUBLIC",
    "sso_role": "",
}

MV_SUBSTATION_PARAMETERS = {
    "delete_at_start": True,
    "name": "MV Substations",
    "description": "Substations with output voltage range 1000-35000V",
    "category": "FEASIBILITY",
    "list_on_fe": False,
}

FEEDER_PARAMETERS = {
    "delete_at_start": False,
    "name": "MV Feeders",
    "description": "1000-35000V feeders (power cables)",
    "category": "FEASIBILITY",
    "list_on_fe": False,
}


class ScriptRunner(AbstractScriptRunner):
    validation = {}
    bucket_name = "data_lake_core"
    date = dt.date(2023, 10, 17)
    datasets = {
        "substation": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/Substations/MapServer/0/query",
        "feeder_3ph_load_capacity": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/EV_Load_Serving_Capacity/MapServer/0/query",
        "feeder_1_2ph_no_load_capacity": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/EV_Load_Serving_Capacity/MapServer/1/query",
        "feeder_3ph_oh_overview": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/DistAssetsOverview/MapServer/1/query",
        "feeder_3ph_ug_overview": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/DistAssetsOverview/MapServer/2/query",
        "feeder_1_2ph_overview": "https://systemdataportal.nationalgrid.com/arcgis/rest/services/NYSDP/DistAssetsOverview/MapServer/3/query",
    }
    MV_SUBSTATION_PARAMETERS["publish_date"] = date
    FEEDER_PARAMETERS["publish_date"] = date

    @classmethod
    def run(cls, con: Connection):
        """Run the inserts / updates / alters."""
        dl_reader = ArcGISRestApiDLReader(
            gc_client,
            cls.bucket_name,
            "/core/national_grid_ny/",
            data_dir,
            cls.date,
            log,
        )

        gdf_3ph_feeder = dl_reader.get_data(
            "feeder_3ph_load_capacity",
            base_url=cls.datasets.get("feeder_3ph_load_capacity"),
            order_by_fields="OBJECTID",
            use_saved=True,
        )
        gdf_1_2ph_feeder = dl_reader.get_data(
            "feeder_1_2ph_no_load_capacity",
            base_url=cls.datasets.get("feeder_1_2ph_no_load_capacity"),
            order_by_fields="OBJECTID",
            chunksize=200,
            use_saved=True,
        )

        gdf_3ph_feeder["phase"] = "3PH"
        gdf_1_2ph_feeder["phase"] = "1PH and 2PH"
        gdf_feeder = concat_gdf([gdf_3ph_feeder, gdf_1_2ph_feeder])

        feeder_3ph_oh_overview = dl_reader.get_data(
            "feeder_3ph_oh_overview",
            base_url=cls.datasets.get("feeder_3ph_oh_overview"),
            order_by_fields="OBJECTID",
            out_fields="OBJECTID,MASTER_CDF,Construction",
            use_saved=True,
        )
        feeder_3ph_ug_overview = dl_reader.get_data(
            "feeder_3ph_ug_overview",
            base_url=cls.datasets.get("feeder_3ph_ug_overview"),
            order_by_fields="OBJECTID",
            out_fields="OBJECTID,MASTER_CDF,Construction",
            use_saved=True,
        )
        feeder_1_2ph_overview = dl_reader.get_data(
            "feeder_1_2ph_overview",
            base_url=cls.datasets.get("feeder_1_2ph_overview"),
            order_by_fields="OBJECTID",
            out_fields="OBJECTID,MASTER_CDF,Construction",
            chunksize=200,
            use_saved=True,
        )

        construction_mapping = (
            {  # mapping to get OH/UG (level) label from "overview" datasets
                row["MASTER_CDF"]: row["Construction"]
                for _, row in concat_gdf(
                    [
                        feeder_3ph_oh_overview,
                        feeder_3ph_ug_overview,
                        feeder_1_2ph_overview,
                    ]
                ).iterrows()
            }
        )

        gdf_feeder["level"] = gdf_feeder["Master_CDF"].apply(
            lambda x: construction_mapping.get(x)
        )
        gdf_feeder["last_updated"] = str(cls.date)
        gdf_feeder["feeder_voltage"] = gdf_feeder["feeder_voltage"] * 1000
        gdf_feeder = add_us_voltage_bucket(gdf_feeder, "bucket", "feeder_voltage")
        gdf_feeder = add_voltage_code_iec_60038(
            gdf_feeder, "voltage_level", "feeder_voltage"
        )
        gdf_feeder["dno"] = "national_grid_ny"

        substation_out_v_mapping = {
            row["substation_bank_name"]: row["feeder_voltage"]
            for _, row in gdf_feeder.iterrows()
        }

        FEEDER_PARAMETERS["additional_properties"] = {
            "feeder_voltage": "voltage",
            "substation_bank_name": "substation",
            "Master_CDF": "circuit",
            "substation_bank_rating": "substation_bank_rating_mw",
            "feeder_peak_load": "peak_load",
            "feeder_rating": "rating",
            "load_capacity_headroom": "dhr",
            "last_updated": "last_updated",
            "level": "level",
            "bucket": "bucket",
            "voltage_level": "voltage_level",
            "dno": "dno",
            "phase": "phase",
        }
        FEEDER_PARAMETERS["string_properties"] = [
            "substation_bank_name",
            "Master_CDF",
            "last_updated",
            "level",
            "bucket",
            "voltage_level",
            "dno",
            "phase",
        ]
        FEEDER_PARAMETERS["source_file_internal_location"] = dl_reader.get_dl_path(
            "feeder_3ph_load_capacity"
        )

        gdf_ss = dl_reader.get_data(
            "substation",
            base_url=cls.datasets.get("substation"),
            order_by_fields="OBJECTID",
        )

        gdf_ss["out_v"] = gdf_ss["NAME"].apply(
            lambda x: substation_out_v_mapping.get(x)
        )
        gdf_ss = add_voltage_code_iec_60038(gdf_ss, "category", "out_v")
        gdf_ss["label"] = gdf_ss[["NAME", "out_v"]].apply(
            lambda x: generate_label(x[0], None, x[1]), axis=1
        )
        gdf_ss["dno"] = "national_grid_ny"
        gdf_ss["last_updated"] = str(cls.date)

        MV_SUBSTATION_PARAMETERS["additional_properties"] = {
            "out_v": "out_v",
            "NAME": "name",
            "last_updated": "last_updated",
            "category": "category",
            "label": "label",
            "dno": "dno",
        }
        MV_SUBSTATION_PARAMETERS["string_properties"] = [
            "NAME",
            "last_updated",
            "category",
            "label",
            "dno",
        ]
        MV_SUBSTATION_PARAMETERS[
            "source_file_internal_location"
        ] = dl_reader.get_dl_path("substation")

        # -------------------------------------------------------------------------------

        gdutil.upload_dataset(
            FEEDER_PARAMETERS,
            DATASET_SOURCE_PARAMETERS,
            gdf_feeder,
            os.path.join(data_dir, "tmp", "feeder.geojson"),
        )
        gdutil.upload_dataset(
            MV_SUBSTATION_PARAMETERS,
            DATASET_SOURCE_PARAMETERS,
            gdf_ss,
            os.path.join(data_dir, "tmp", "substation.geojson"),
        )

    @classmethod
    def validate(cls, con: Connection) -> bool:
        """Validate the data produced in that script's run method action."""
        return True
