"""
CLI script to manage schema evolution on DW objects (views, tables, funcs etc)
* The script is a wrapper:
    on flywaydb - for managing Snowflake onemedical_dw schema
    on HiveCliHook - for managing Hive info_store(_*) schema
* Version History is tracked with metadata table on Snowflake
    flywaydb - OM_DB_<env>.ONEMEDICAL_DW_PROTECTED.MIGRATIONS_ONEMEDICAL_DW
    flywaydb - OM_DB_<env>.ONEMEDICAL_DW_PROTECTED.MIGRATIONS_ONEMEDICAL_DW_ADS
    info_store - OM_DB_<env>.ONEMEDICAL_DW_PROTECTED.MIGRATIONS_INFO_STORE
* Script assumes the environment specified with -e is already setup on Snowflake
Also it expects a programmatic Auth setup for the corresponding user etl_user_<env_name> on SSM

"""
import argparse
import glob
import logging
import os
import subprocess
import sys
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from pathlib import Path

import boto3
import jinja2
import snowflake.connector
import sqlparse

sys.path.append(os.environ.get("AIRFLOW_HOME") + "/plugins")
sys.path.append(os.environ.get("AIRFLOW_HOME") + "/dags")
from lib.common import get_airflow_vars
from livy_hook import LivyHook

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

KEY_PATH = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow") + "/keys"
MIGRATIONS_BASE = os.environ.get("AIRFLOW_HOME") + "/utils/migrations"
FLYWAY_PATH = "/usr/local/flyway/"
sys.path.append(os.environ.get("AIRFLOW_HOME") + "/dags")


class MigrationsManager(ABC):

    MANAGED_SCHEMAS = ["onemedical_dw", "onemedical_dw_ads", "info_store"]
    MANAGED_COMMANDS = ["new", "reset_meta", "migrate", "info"]

    def __init__(self, env_name, schema_name, command):
        """
        Usage: utils/migrations/migrations.py -s SCHEMA_NAME -e ENV_NAME -c COMMAND

        Arguments:
        -s, --schema_name     target schema name [onemedical_dw|info_store]
        -e, --env_name        environment name [<dev-user>|integration|production]
        -c, --command         migration command [new|reset_meta|migrate|info|repair|baseline]
                                info_store supports only "new", "migrate"
        -h, --help            show this help message and exit

        Examples:
            # generate an empty versioning file under migrations/sql/onemedical_dw/
            python utils/migrations/migrations.py -e smadhavan -s onemedical_dw -c new

            # view current status from tracking table om_db_smadhavan.onemedical_dw.flyway_schema_history
            python utils/migrations/migrations.py -e smadhavan -s onemedical_dw -c info

            # generate an empty versioning file under migrations/sql/info_store/
            python utils/migrations/migrations.py -e smadhavan -s info_store -c new

            # apply new migrations found in sql/info_store/ to info_store_smadhavan
            python utils/migrations/migrations.py -s info_store -e smadhavan -c migrate

            # apply new migrations found in sql/info_store/ to info_store_integration
            python utils/migrations/migrations.py -s info_store -e integration -c migrate

        """
        self.env_name = env_name
        self.schema_name = schema_name
        self.command = command
        self._validate_schema()
        self._validate_command()

    def _validate_schema(self):
        return self.schema_name in self.MANAGED_SCHEMAS

    def _validate_command(self):
        return self.command in self.MANAGED_COMMANDS

    def run_new(self):
        ts = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
        schema_base = self.schema_name

        tmp_file = f"{MIGRATIONS_BASE}/sql/{schema_base}"
        if not Path(tmp_file).exists():
            Path(tmp_file).mkdir(parents=True, exist_ok=True)

        tmpl_name = f"V{ts}__migration_descriptive_name.sql"
        Path(f"{tmp_file}/{tmpl_name}").touch()

        logging.info(f"NEW Migration Template created at: {tmp_file}/{tmpl_name}")

        return ts, tmp_file

    # @abstractmethod
    # def commit_migrations(self, version):
    #     pass

    @abstractmethod
    def run_migrate(self):
        pass

    @abstractmethod
    def run_info(self):
        pass

    @abstractmethod
    def run_reset_meta(self):
        pass

    def get_pending_new_migrations(self, sql_path, recent_dbvers):

        if not recent_dbvers or len(recent_dbvers) < 1:
            recent_dbvers = [19000101000000]

        fileset = [file for file in glob.glob(sql_path + "**/V*.sql", recursive=True)]
        delta_fileset = []
        for file_ver in fileset:
            ver = int(os.path.basename(file_ver).split("__")[0][1:])
            if ver >= recent_dbvers[-1] and ver not in recent_dbvers:
                delta_fileset.append(file_ver)

        return delta_fileset

    def run(self):
        return getattr(self, "run_" + self.command, lambda: self.usage)()

    def usage(self):
        logging.info(MigrationsManager.__init__.__doc__)


class SnowflakeMigrationsUtil:
    def __init__(
        self,
        env_name,
        schema_name,
        history_table_schema="onemedical_dw_protected",
        history_table_name="migrations",
    ):
        """initialize object for class SnowflakeMigrationUtils

        Args:
            env_name (str): database/environment name
            schema_name (str): schema name
            history_table_schema (str, optional): schema where version history tracking table is created.
                Defaults to 'onemedical_dw'.
            history_table_name (str, optional): version history tracking table name.
                Defaults to 'info_store_migrations'.
        """
        # super().__init__(env_name, schema_name)
        self.env_name = env_name
        self.schema_name = schema_name
        self.history_table_name = f"migrations_{self.schema_name}"
        self.history_table_schema = history_table_schema
        self.conn = self.set_snowflake_conn()

    def get_pvtkey_ssm(self):
        ssm = boto3.client("ssm", region_name="us-east-1")

        if self.env_name is None:
            return None

        ssm_key = f"/production/data-storm/default/snowflake/etl_user_{self.env_name}/rsa_private_key"

        private_key = ssm.get_parameter(Name=ssm_key, WithDecryption=True)["Parameter"][
            "Value"
        ]

        local_key_path = f"{KEY_PATH}/rsa_key_etl_user_{self.env_name}.pem"
        with open(local_key_path, "w") as fh:
            fh.write(private_key)

        return local_key_path

    def set_snowflake_conn(self):

        local_key_path = self.get_pvtkey_ssm()

        with open(f"{local_key_path}", "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(), password=None, backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            user=f"etl_user_{self.env_name}",
            role=f"etl_user_{self.env_name}_role",
            warehouse="etl_dev_wh" if self.env_name != "production" else "etl_prod_wh",
            account="onemedical.us-east-1",
            database=f"om_db_{self.env_name}",
            schema=f"{self.history_table_schema}",
            private_key=pkb,
        )

    def get_snowflake_conn(self):
        if not self.conn:
            self.set_snowflake_conn(self.env_name)

        return self.conn

    def get_last_migration_dbver(
        self, schema_name=None, table_name=None, history=False
    ):
        if schema_name:
            self.history_table_schema = schema_name
        if table_name:
            self.history_table_schema = table_name

        if history:
            cur = self.conn.cursor().execute(
                f"""
                    SELECT version FROM
                    {self.history_table_schema}.{self.history_table_name}
                    ORDER BY 1 DESC LIMIT 100
                """
            )

            return [rec[0] for rec in cur]
        else:
            cur = self.conn.cursor().execute(
                f"SELECT max(version) from {self.history_table_schema}.{self.history_table_name}"
            )
            return [rec[0] for rec in cur]

    def update_migration_ver(self, ver, schema_name=None, table_name=None):
        if schema_name:
            self.history_table_schema = schema_name
        if table_name:
            self.history_table_schema = table_name

        return self.conn.cursor().execute(
            f"INSERT INTO {self.history_table_schema}.{self.history_table_name} values({ver})"
        )

    def create_meta_table(self, schema_name=None, table_name=None):
        if schema_name:
            self.history_table_schema = schema_name
        if table_name:
            self.history_table_schema = table_name

        self.conn.cursor().execute(
            "CREATE TABLE IF NOT EXISTS "
            + f"{self.history_table_schema}.{self.history_table_name} (version number)"
        )

    def drop_meta_table(self, schema_name=None, table_name=None):
        if schema_name:
            self.history_table_schema = schema_name
        if table_name:
            self.history_table_schema = table_name

        self.conn.cursor().execute(
            f"DROP TABLE IF EXISTS {self.history_table_schema}.{self.history_table_name}"
        )

    def close(self):
        if self.conn:
            self.conn.close()


class FlywayMigrationsUtil:
    def __init__(self, env_name, schema_name):
        self.env_name = env_name
        self.schema_name = schema_name
        self.sf_util = SnowflakeMigrationsUtil(
            env_name,
            schema_name,
            history_table_schema="ONEMEDICAL_DW_PROTECTED",
            history_table_name="FLYWAY_SCHEMA_HISTORY",
        )

    def get_flyway_conn_uri(self):
        # setup and use flyway cli for onemedical_dw
        # flyway bug requires password to be set mandatorily
        os.environ["FLYWAY_USER"] = f"etl_user_{self.env_name}"
        os.environ["FLYWAY_PASSWORD"] = ""

        local_key_path = self.sf_util.get_pvtkey_ssm()
        wh = "etl_dev_wh" if self.env_name != "production" else "etl_prod_wh"
        flyway_jdbc_uri = (
            "jdbc:snowflake://onemedical.us-east-1.snowflakecomputing.com/"
            + f"?private_key_file={local_key_path}&db=om_db_{self.env_name}&warehouse={wh}"
            + f"&schema={self.schema_name}&role=etl_user_{self.env_name}_role&authenticator=snowflake_jwt"
        )

        return flyway_jdbc_uri

    def run_flyway_command(self, command):
        flyway_jdbc_uri = self.get_flyway_conn_uri()
        conf_base_name = self.schema_name.replace("_", "-")

        run_cmd = [
            f"{FLYWAY_PATH}/flyway",
            f"-url={flyway_jdbc_uri}",
            f"-configFiles={MIGRATIONS_BASE}/flywaydb/conf/flyway-{conf_base_name}.conf",
            f"-locations=filesystem:{MIGRATIONS_BASE}/sql/{self.schema_name}/",
            f"{command}",
        ]
        try:
            subprocess.call(run_cmd)
        except Exception as e:
            logging.exception(f"Error running migrations: {run_cmd} with {e}")
            raise

        return 1

    def reset_meta(self):
        self.sf_util.drop_meta_table()


class DWMigrationsManager(MigrationsManager):
    def __init__(self, env_name, schema_name, command):
        super().__init__(env_name, schema_name, command)
        self.mig_util = FlywayMigrationsUtil(env_name, schema_name)

    def run_migrate(self):
        # migrate
        self.mig_util.run_flyway_command(self.command)

    def run_info(self):
        # info
        return self.mig_util.run_flyway_command(self.command)

    def run_reset_meta(self):
        # Note: this performs custom action not available via std flyway cmds
        self.mig_util.reset_meta()

        self.mig_util.run_flyway_command("baseline")


class InfoStoreMigrationsManager(MigrationsManager):
    def __init__(self, env_name, schema_name, command):
        super().__init__(env_name, schema_name, command)
        if command != "new":
            self.mig_util = SnowflakeMigrationsUtil(
                env_name, schema_name, history_table_name="migrations_info_store"
            )

    def render_file(self, file_text):
        # some cleanup of hive-like syntax, if any
        file_text = file_text.replace("`", "").replace("var.value.", "")

        template = jinja2.Template(file_text)
        return template.render(**get_airflow_vars(), **{"env_name": f"{self.env_name}"})

    def gen_sparksql(self, file_text):
        """content of .sql file is split into list of sql's if it has multiple sql's

        Args:
            file_text (str): sql file text

        Returns:
            list: list of sql's ready for spark.sql()
        """
        # file_text could have multiple ddls - step thru those using sqlparse
        return [
            f'spark.sql("""{sql.replace(";", "")}""")'
            for sql in sqlparse.split(self.render_file(file_text).strip())
            if len(sql.strip()) > 0
        ]

    # handle migrate command for info_store
    def run_migrate(self):

        recent_dbvers = self.mig_util.get_last_migration_dbver(history=True)
        last_dbver = recent_dbvers[0] if len(recent_dbvers) > 0 else None

        # compare last registered db version vs version on fs,
        # if fs-version > last-db-version, we apply the delta
        new_migration_files = self.get_pending_new_migrations(
            f"{MIGRATIONS_BASE}/sql/{self.schema_name}/", recent_dbvers
        )
        if not new_migration_files:
            logging.info(
                f"No new migrations found - last successful dbversion: {last_dbver}"
            )
            sys.exit(0)

        for sql_file in new_migration_files:
            # read sql text
            with open(sql_file) as fh:
                sql_text = fh.read()

            ver = os.path.basename(sql_file).split("__")[0][1:]
            logging.info(f"Applying migration file {sql_file} with ver: {ver}")

            # call Hivecli for sql
            hook = LivyHook(task_id="run_migrate", timeout=600)

            try:
                # logging.info(f"Rendered sql_text: {self.render_file(sql_text)}")
                hook.run("\n".join(self.gen_sparksql(sql_text)))
            finally:
                hook.close_session()

            # applied sql_file with no exceptions, update the version
            self.commit_migrations(ver)

    def commit_migrations(self, version):
        return self.mig_util.update_migration_ver(version)

    def run_info(self):
        logging.info(
            f"Latest db_versions found: {self.mig_util.get_last_migration_dbver(history=True)[:10]}"
        )

    def run_reset_meta(self):
        self.mig_util.drop_meta_table()
        self.mig_util.create_meta_table()
        logging.info("info_store migrations metadata reset completed!")


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--schema_name",
        type=str,
        help="Target schema_name [onemedical_dw|info_store(_*)]",
        required=True,
    )
    parser.add_argument(
        "-e",
        "--env_name",
        type=str,
        help="Target environment [<dev_userid>|integration|production]",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        required=True,
        help="Command to run [reset_meta|new|migrate|info|repair|baseline]",
    )

    args = parser.parse_args()

    # setup commandline params
    schema_name = args.schema_name
    env_name = args.env_name
    command = args.command

    # mig = MigrationsManager(env_name, schema_name, command)
    if "onemedical_dw" in schema_name:
        mig = DWMigrationsManager(env_name, schema_name, command)

        # commands only applicable for flyway managed targets
        # run_migrate psuedo call takes care of appropriate command
        if command in ["repair", "baseline"]:
            mig.run_migrate()
            sys.exit(0)
    else:
        mig = InfoStoreMigrationsManager(env_name, schema_name, command)

    if command in mig.MANAGED_COMMANDS:
        try:
            mig.run()
        except Exception:
            raise
    else:
        mig.usage()


if __name__ == "__main__":
    main()
