import json

import geopandas as gpd
import numpy as np
from shapely import wkt
import datetime as dt

import pandas as pd
from sqlalchemy.engine.base import Connection
from db_version.abstract_script_runner import AbstractScriptRunner
from utils.dcl.data_lake_reader import DLReader
from config import gc_client, log, data_dir
from utils.dcl.db.db_insert import insert_dataset
from utils.dcl.dno_general import generate_label
from utils.dcl.db.db_insert import insert_dataset_metadata, insert_dataset_coverage


class ScriptRunner(AbstractScriptRunner):
    validation = {}
    bucket_name = "data_lake_core"
    dl_path = "/core/ng/headroom/20251016/WPD Network Capacity Map 16-01-2025.csv"
    date = dt.date(2025, 1, 16)

    @classmethod
    def run(cls, con: Connection):
        con.execute(
            """
            DELETE FROM dataset.dataset_source WHERE dataset_id IN (SELECT dataset_id FROM dataset.dataset WHERE table_name LIKE '%%ng/substation/%%');
            DELETE FROM dataset.dataset WHERE table_name LIKE '%%ng/substation/%%';
            DELETE FROM dataset.dataset_coverage WHERE dataset LIKE '%%ng/substation/%%';
            DROP TABLE IF EXISTS dataset."core/ng/substation/pss" CASCADE;
            DROP TABLE IF EXISTS dataset."core/ng/substation/bsp" CASCADE;
            """
        )

        dl_reader = DLReader(gc_client, cls.bucket_name, data_dir, log)
        gdf = dl_reader.read_csv(
            cls.dl_path,
            use_saved=True,
            xcol="Longitude",
            ycol="Latitude",
            crs="EPSG:4326",
        )

        columns = {
            "Network Reference ID": "network_reference_id",
            "Substation Name": "name",
            "Parent Network Reference ID": "parent_id",
            "Substation Number": "substation_number",
            "Asset Type": "asset_type",
            "Group": "gsp",
            "Upstream Voltage": "voltage_str",
            "Downstream Voltage": "out_v",
            "Fault Level Headroom": "fault_level_headroom",
            "Firm Capacity of Substation (MVA)": "firm_capacity_of_substation_mva",
            "Reverse Power Capability (MVA)": "reverse_power_capability_mva",
            "Measured Peak Demand (MVA)": "measured_peak_demand_mva",
            "Demand Headroom (MVA)": "dhr",
            "Demand Headroom RAG": "dhr_category",
            "Upstream Demand Headroom RAG": "upstream_demand_headroom_rag",
            "Upstream Demand Headroom": "upstream_demand_headroom",
            "geometry": "geometry",
        }

        gdf = gdf[list(columns.keys())]
        gdf.rename(columns=columns, inplace=True)

        gdf.drop_duplicates(subset=["network_reference_id"], inplace=True)

        gdf["network_reference_id"] = gdf["network_reference_id"].astype(int)
        gdf["parent_id"] = gdf["parent_id"].apply(
            lambda x: int(x) if not pd.isna(x) else None
        )

        gdf["in_v"] = gdf["voltage_str"].apply(
            lambda x: None
            if pd.isna(x)
            else float(x.split("/")[0]) * 1000
            if "/" in x
            else float(x)
        )
        gdf["out_v"] = gdf["out_v"].astype(float) * 1000
        gdf["label"] = gdf[["name", "in_v", "out_v"]].apply(
            lambda x: generate_label(*x), axis=1
        )
        gdf["dno"] = "ng"
        gdf["last_updated"] = str(dt.date(2024, 7, 24))

        pss = gdf[gdf["asset_type"] == "Primary"].copy()
        bsp = gdf[gdf["asset_type"] == "BSP"].copy()

        pss = pss.merge(
            bsp[["network_reference_id", "name"]].rename(
                columns={"network_reference_id": "parent_id", "name": "bsp"}
            ),
            how="left",
            on="parent_id",
        )

        pss["category"] = "pss"
        bsp["category"] = "bsp"

        more_info = {
            "network_reference_id": "Network Reference ID",
            "substation_number": "Substation Number",
            "gsp": "Group",
            "fault_level_headroom": "Fault Level Headroom",
            "firm_capacity_of_substation_mva": "Firm Capacity of Substation (MVA)",
            "reverse_power_capability_mva": "Reverse Power Capability (MVA)",
            "measured_peak_demand_mva": "Measured Peak Demand (MVA)",
            "upstream_demand_headroom_rag": "Upstream Demand Headroom RAG",
            "upstream_demand_headroom": "Upstream Demand Headroom",
        }

        bsp["more_info"] = list(
            map(
                json.dumps,
                bsp[list(more_info.keys())]
                .rename(columns=more_info)
                .replace({np.nan: None})
                .to_dict(orient="records"),
            )
        )
        bsp.drop(
            columns=[k for k in more_info.keys() if k != "network_reference_id"],
            inplace=True,
        )

        more_info["bsp"] = "Bulk Supply Point"
        pss["more_info"] = list(
            map(
                json.dumps,
                pss[list(more_info.keys())]
                .rename(columns=more_info)
                .replace({np.nan: None})
                .to_dict(orient="records"),
            )
        )
        pss.drop(
            columns=[k for k in more_info.keys() if k != "network_reference_id"],
            inplace=True,
        )

        pss.drop(columns=["voltage_str", "parent_id", "asset_type"], inplace=True)
        bsp.drop(columns=["voltage_str", "parent_id", "asset_type"], inplace=True)

        insert_dataset(
            pss,
            "core.ng.substation.pss.v2024_07",
            con,
            replace=True,
            xref_cols=["network_reference_id"],
        )
        insert_dataset(
            bsp,
            "core.ng.substation.bsp.v2024_07",
            con,
            replace=True,
            xref_cols=["network_reference_id"],
        )

        ng_polygon = wkt.loads(
            con.execute(
                """
                SELECT ST_AsText(ST_Union(geometry)) 
                FROM dataset."core/neso/uk_dno_areas" 
                WHERE properties ->> 'dno_abb' = 'WPD';
                """
            ).fetchone()[0]
        )

        insert_dataset_coverage("core/ng/substation/pss/v2024_07", con, ng_polygon)
        insert_dataset_coverage("core/ng/substation/bsp/v2024_07", con, ng_polygon)

        insert_dataset_metadata(
            con,
            "dataset",
            override_row=True,
            source_name="NG",
            dataset_name="NG_PSS",
            dataset_table_name="core/ng/substation/pss/v2024_07",
            dataset_description="PSSs of NG (ex WPD) with demand headroom and also RAG dhr category;",
            inserted_on=cls.date,
            updated_on=cls.date,
            brackets_to_calculate=["in_v", "out_v", "dhr"],
        )

        insert_dataset_metadata(
            con,
            "dataset",
            override_row=True,
            source_name="NG",
            dataset_name="NG_BSP",
            dataset_table_name="core/ng/substation/bsp/v2024_07",
            dataset_description="BSPs of NG (ex WPD) with demand headroom and also RAG dhr category;",
            inserted_on=cls.date,
            updated_on=cls.date,
            brackets_to_calculate=["in_v", "out_v", "dhr"],
        )

    @classmethod
    def validate(cls, con: Connection) -> bool:
        return True
