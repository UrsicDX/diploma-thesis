""" 
Like v0205 but for substations
"""
from sqlalchemy.engine.base import Connection
from db_version.abstract_script_runner import AbstractScriptRunner
import os

from config import email_to, log, mailer
from utils.dcl.dataset import add_leaf_dataset


class ScriptRunner(AbstractScriptRunner):
    validation = {}

    @classmethod
    def run(cls, con: Connection):
        """Run the inserts / updates / alters."""
        substation_levels = [
            "gsp",
            "bsp",
            "pss",
            "lss",
        ]
        dno_geometry_columns = {
            "enw": "geometry",
            "ssen": "geometry",
            "ng": "geometry",
            "np": "geometry",
        }
        # We do not have data for all substation levels for all DNOs
        skip_dno_levels = [("ng", "gsp"), ("ng", "lss")]
        dno_list = sorted(dno_geometry_columns.keys())

        # add_leaf_dataset(

        for substation_level in substation_levels:
            # Create view
            sql = f"""CREATE OR REPLACE VIEW dataset."core/display/dno/substation/{substation_level.lower()}" AS ("""
            sql += " UNION ALL ".join(
                [
                    f"""
                    SELECT 
                        properties || jsonb_build_object('dno', '{dno}') AS properties,
                        {dno_geometry_columns[dno]} AS geometry,
                        valid_from, 
                        valid_to, 
                        entity_id, 
                        xref
                    FROM dataset."core/{dno}/substation/{substation_level.lower()}"
                    """
                    for dno in dno_list
                    if (dno, substation_level) not in skip_dno_levels
                ]
            )
            sql += """);"""

            con.execute(sql)

            add_leaf_dataset(
                f"core.display.dno.substation.{substation_level.lower()}",
                con,
                description=f"Combined view of {substation_level} cables from dno(s): "
                + " ".join(dno_list),
                source=" ".join(
                    [
                        f"core.{dno}.power_cable.{substation_level.lower()}"
                        for dno in dno_list
                    ]
                ),
            )

        mailer.send(email_to, os.path.basename(__file__)[:-3], "OK.")

    @classmethod
    def validate(cls, con: Connection) -> bool:
        """Validate the data produced in that script's run method action."""
        return True
