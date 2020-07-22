import os
import random
import re
import string

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from lib import spark_pkg_helpers
from lib.aws import delete_objects
from livy_batch_hook import LivyBatchHook


class SparkLivyBatchOperator(BaseOperator):
    """
    Executes spark code through Livy batch API. Use for pipelines spanning multiple files.
    For dynamic code execution (inline code as string), use SparkCodeOperator.

    Args:
        driver_path (str):          Spark driver path
        driver_args (list):         optional list of arguments, passed to driver script
        job_conf_str (dict):        (optional) job configuration to pass when creating livy session
        job_conf_path (str):        (optional) file with job configuration to pass when creating livy session

    Returns:
        str: final .zip file path

    """

    @apply_defaults
    def __init__(
        self,
        driver_path=None,
        driver_args=[],
        job_conf=None,
        job_conf_path=None,
        repo="data-storm",
        timeout=1800,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)

        if driver_path is None:
            raise AirflowException("ERROR: driver_path is Missing!")

        self.airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

        self.job_conf = job_conf
        self.job_conf_path = job_conf_path
        self.driver_path = driver_path
        self.driver_args = driver_args
        self.repo = repo

        # self.job_conf = {}

        self.task_id = kwargs["task_id"]
        self.s3_source_code_path = Variable.get("s3_data_lake_root") + "/storm/src/"

        self.timeout = timeout

    def execute(self, context):
        self.execution_date = context.get("execution_date").strftime("%Y-%m-%d")

        self.job_conf = spark_pkg_helpers.JobConfSetup(
            job_conf=self.job_conf,
            driver_path=self.driver_path,
            driver_args=self.driver_args,
        ).initialize_job()

        if self.repo == "data_pipelines_ads" and self.repo not in self.job_conf["file"]:
            self.job_conf["file"] = "dags/data_pipelines_ads/" + self.job_conf["file"]

        # livy 0.6 uses job_name to uniquely identify sessions
        uniq_id = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
        )

        script = spark_pkg_helpers.JobPackager(
            self.__class__.__name__ + "__" + self.task_id + "_" + uniq_id,
            self.execution_date,
            self.job_conf,
            self.airflow_home,
            self.s3_source_code_path,
            {**context, "params": {**self.params}},
        ).package_files()
        script = re.sub("password ?= ?.*?,", 'password="****",', script)

        self.log.info(f"Driver Submitted: {script}")
        try:
            self.hook = LivyBatchHook(
                task_id=self.task_id, job_conf=self.job_conf, timeout=self.timeout
            ).submit_job()
        except Exception as e:
            self.log.info(f"Driver FAILED: {script}\n\n{str(e)}")
            raise
        finally:
            # Always clean up, even on failed jobs
            self.cleanup(self.job_conf["file"])

        self.log.info(f"SparkBatchOperator execute COMPLETED for {self.task_id}")

    def cleanup(self, driver_path):
        if not re.match(r"s3://.*/storm/src/tmp.*", driver_path):
            return

        driver_base_path = re.sub(
            r"(s3://.*/storm/src/tmp/.*?/).*", r"\g<1>", driver_path
        )
        self.log.info(f"***Cleaning up workspace: {driver_base_path}")

        delete_objects(driver_base_path)


class SparkLivyBatchPlugin(AirflowPlugin):
    name = "spark_livy_batch_plugin"
    operators = [SparkLivyBatchOperator]
