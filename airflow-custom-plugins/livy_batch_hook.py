import json
import re
import time

import paramiko
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
from lib.contextmanagers import timethis
from lib.log import get_logger

log = get_logger()


class LivyBatchHook(BaseHook):
    """Class encapsulates functionality to interface with Livy /batches API.
        For use in SparkLivyBatchOperator

    Args:
        BaseHook ([type]): [description]

    Raises:
        AirflowException: [description]

    Returns:
        [type]: [description]
    """

    default_job_conf = {
        "driverMemory": "2G",
        "driverCores": 2,
        "executorCores": 2,
        "executorMemory": "2G",
        "numExecutors": 1,
        "queue": "default",
    }

    headers = {"Content-Type": "application/json"}

    # amount of time to attempt reconnect when creating spark session

    def __init__(self, task_id="", job_conf=None, job_conf_path=None, timeout=30 * 60):
        """
        Instantiate LivyBatchHook object from given job configuration and submits the job to livy /batches api.

        :param task_id:             Unique identifier for task. Will appear in Yarn manager
        :type task_id:              string

        :param job_conf:            Overrides for spark task configuration.
                                    Documentation: https://livy.incubator.apache.org/docs/latest/rest-api.html
        :type job_conf:             dict

        :param timeout:             Max timeout for spark task execution. Default is 30 minutes
        :type timeout:              int
        """

        if job_conf is not None:
            self.job_conf = {**self.default_job_conf, **job_conf}
        else:
            self.job_conf = self.default_job_conf

        # TODO go get job_conf_path
        self.task_id = task_id
        self.timeout = timeout
        self.run_env = Variable.get("run_env")
        self.livy_url = (
            "http://"
            + Variable.get("spark_livy_host")
            + ":"
            + Variable.get("spark_livy_port")
        )

        self.batch_url = self.livy_url + "/batches"

    def submit_job(self):
        """Submit a job using a valid job_conf object and track the job status

        Raises:
            AirflowException: [description]
        """
        if not self.job_conf or ("file" not in self.job_conf):
            raise AirflowException("Missing arg: job specification is empty")

        r = requests.post(
            self.batch_url, data=json.dumps(self.job_conf), headers=self.headers
        )

        with timethis("spark_livy_batch_job - " + str(r.json()["id"])):
            ret = self.verify_job_status(r)

            log.info(f"Job Status Response: {ret}")

        self.close_session()

    def convert_to_ip(self, url):
        """Convert EMR internal hostname to IP address format
        Example: ip-10-0-13-163.ec2.internal:8042 is converted to 10.0.13.163:8042

        Args:
            url (str): any EC2.internal url string

        Returns:
            [type]: [description]
        """
        return re.sub(
            r"http://ip-(\d+)-(\d+)-(\d+)-(\d+).ec2.internal(.*)",
            r"http://\1.\2.\3.\4\5",
            url,
        )

    def handle_redirect(self, url):
        """Follows and resolves url redirects(proxies) and return final url

        Args:
            url (str): any url string

        Returns:
            [type]: [description]
        """
        response = requests.get(url, allow_redirects=False)

        if response.is_redirect:
            return self.handle_redirect(
                self.convert_to_ip(response.headers["Location"])
            )

        else:
            return url

    def verify_job_status(self, r):

        timeout = (
            time.time() + self.timeout
        )  # 30min grace period for job to be accepted

        r = r.json()
        batch_id = r["id"]

        while True:
            resp = requests.get(self.batch_url + "/" + str(batch_id) + "/state")
            resp = resp.json()

            if type(resp) is str:  # handle mesg of kind - Session '85' not found.
                return resp

            # sed  '/YARN executor launch context/,/=====/d; / INFO /d; /GC /d; /CMS-concurrent/d'
            if resp["state"] in ["dead", "error", "failed"]:
                user = "hadoop"
                host = Variable.get("spark_livy_host")
                cmd = f"""
                export HADOOP_USER_NAME=livy;
                yarn logs -applicationId {self.app_id} | egrep -v 'INFO |CMS-concurrent'
                """

                time.sleep(5)
                err_str = self.get_app_logs(host, user, cmd)

                # full_log = self.collect_failure_logs(batch_id)
                # full_log = outlog.decode("utf-8") + "\n\n" + errlog.decode("utf-8")
                livy_batch_url = self.batch_url + "/" + str(batch_id)
                raise Exception(
                    f"Ran into an error: {err_str} running the Livy Batch Job: {livy_batch_url}"
                )

            elif resp["state"] in ["success"]:
                return json.dumps(resp)

            elif resp["state"] in ["starting", "busy", "idle", "available"]:
                self.set_driver_metadata(batch_id)

            elif resp["state"] in ["running"]:
                log.info(f"Waiting for application to complete {self.app_id}")

            else:
                raise AirflowException(
                    "Unknown status while waiting for job to start"
                    + json.dumps(resp, indent=4, sort_keys=True)
                )

            if time.time() > timeout:
                raise AirflowException(
                    "Timeout after %s seconds waiting for job run state"
                    % str(self.timeout)
                )

            time.sleep(4)

    def set_driver_metadata(self, batch_id):
        self.batch_session_url = self.batch_url + "/" + str(batch_id)
        batch_resp = requests.get(self.batch_session_url)
        batch_resp = batch_resp.json()

        if hasattr(self, "driver_log_url") and self.driver_log_url:
            return

        log.info(
            "Waiting for application to be accepted..."
            + str(batch_resp["appInfo"]["driverLogUrl"])
        )

        if "appId" in batch_resp:
            self.app_id = batch_resp["appId"]

        if (
            "driverLogUrl" in batch_resp["appInfo"]
            and batch_resp["appInfo"]["driverLogUrl"] is not None
        ):
            self.driver_log_url = batch_resp["appInfo"]["driverLogUrl"]

    def collect_driver_logs(self):
        driverlog = ""

        # collect driverlog
        if (
            hasattr(self, "driver_log_url")
            and self.driver_log_url is not None
            and "http:" in self.driver_log_url
        ):
            final_url = self.handle_redirect(self.convert_to_ip(self.driver_log_url))

            skip = True
            for line in requests.get(final_url, allow_redirects=False).iter_lines():
                line = str(line)
                if skip and not re.match(
                    r"^.*(Showing \d+ bytes of \d+ total(.*))$|^.*( ERROR ApplicationMaster:.*)$",
                    str(line),
                ):
                    continue
                else:
                    skip = False

                driverlog += line + "\n"

        return driverlog

    def collect_failure_logs(self, batch_id):
        """collect failure logs from two end points - driver logs, app logs

        Args:
            batch_id (int): identifier for current batch request

        Returns:
            [type]: [description]
        """
        applog, driverlog = "", ""

        # collect applog
        app_log_url = self.batch_url + "/" + str(batch_id) + "/log"
        final_url = self.handle_redirect(self.convert_to_ip(app_log_url))

        r = requests.get(final_url, allow_redirects=False)
        if "log" in r.json():
            applog = "\n".join(r.json()["log"])

        driverlog = self.collect_driver_logs()

        return applog + "\n" + driverlog + "\n"

    def close_session(self):
        r = requests.delete(self.batch_session_url, headers=self.headers)
        log.info("session closed")

        return r

    # should be moved to lib.common, but currently causes issues with circular plugin ref
    def get_app_logs(self, host, user, cmd, timeout=10):
        with self.get_ssh_conn(hostname=host, username=user) as ssh_client:
            stdin, stdout, stderr = ssh_client.exec_command(cmd, get_pty=True)

            err_str = ""
            for line in stdout:
                err_str += line

            stdout.close()
            stderr.close()

            match = re.search(
                "Traceback .*?(?=Container:|End of LogType)",
                err_str,
                re.MULTILINE | re.DOTALL,
            )

            if match:
                err_str = match.group(0)
            else:
                # err_str = err_str.decode("utf-8").strip("\n")
                except_err = re.search(
                    "Exception:.*?(?=java.lang.Thread.run)",
                    err_str,
                    re.MULTILINE | re.DOTALL,
                ).group(0)
                err_str = except_err

            return err_str

    def get_ssh_conn(
        self,
        hostname=None,
        username=None,
        password=None,
        key_file="keys/onemedical-data-engineering.pem",
        port=22,
        timeout=10,
        keepalive_interval=30,
    ):
        """
        Opens a ssh connection to the remote host.

        :rtype: paramiko.client.SSHClient
        """

        log.info("Creating SSH client")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname,
            username=username,
            password=password,
            key_filename=key_file,
            timeout=timeout,
            port=port,
        )

        if keepalive_interval:
            client.get_transport().set_keepalive(keepalive_interval)

        return client


class LivyHookPlugin(AirflowPlugin):
    name = "livy_hook_plugin"
    hooks = [LivyBatchHook]
