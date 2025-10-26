from sqlalchemy.engine.base import Connection
from db_version.abstract_script_runner import AbstractScriptRunner
import os

from config import email_to, mailer
from utils.dcl.dataset import add_leaf_dataset, make_table_name


class ScriptRunner(AbstractScriptRunner):
    @classmethod
    def run(cls, con: Connection):
        """Run the inserts / updates / alters."""
        for taxonomy in ["core.ng.substation.bsp", "core.ng.substation.pss"]:
            table_name = make_table_name(taxonomy)
            con.execute(
                f"""
                UPDATE dataset."{table_name}"
                SET properties = (properties - 'demand_headroom_mva') || jsonb_build_object('dhr', (properties -> 'demand_headroom_mva')::float)
                WHERE properties ->> 'demand_headroom_mva' IS NOT NULL;
                """
            )
            add_leaf_dataset(taxonomy, con)

        mailer.send(email_to, os.path.basename(__file__)[:-3], "OK.")

    @classmethod
    def validate(cls, con: Connection) -> bool:
        """Validate the data produced in that script's run method action."""
        return True
