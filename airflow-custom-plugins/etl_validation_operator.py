from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class EtlValidationOperator(BaseOperator):
    """
    Executes queries via MySqlHook, compare, assert results.

    :param conn_id1:            db source connection-id
    :type conn_id1:             string

    :param conn_id2:            db target connection-id
    :type conn_id2:             string

    :param op:                  comparison operator(assertTrue, cmpEquals, cmpGreaterThan, cmpDelta).
    :type op:                   string

    :param sql1:                MySql query to execute.
    :type sql1:                 string

    :param sql2:                MySql query to compare.
    :type sql2:                 string

    :param test_suite:          group of test cases with corresponding assertion queries
    :type test_suite:           dict: {'test_cases': [ {'name': 'n1', 'sql': 'sql1'}, {'name': 'n2', 'sql': 'sql2'}] }

    :param delta_perc:          value indicates percentage delta between two query results
    :type delta_perc:           int

    :param timeout:             Max timeout for spark task execution. Default is 30 minutes
    :type timeout:              int
    """

    template_fields = ["sql1", "sql2", "test_suite"]

    @apply_defaults
    def __init__(
        self,
        conn_id1,
        conn_id2=None,
        op="assertTrue",
        sql1=None,
        sql2=None,
        timeout=30 * 60,
        delta_perc=10,
        test_suite={},
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)

        self.conn_id1 = conn_id1
        self.conn_id2 = conn_id2
        self.op = op
        self.sql1 = sql1
        self.sql2 = sql2
        self.delta_perc = delta_perc
        self.test_suite = test_suite
        self.timeout = timeout
        self.task_id = kwargs["task_id"]

    def execute(self, context):
        from lib.common import get_hook

        if self.conn_id1 is None or (self.sql1 is None and len(self.test_suite) == 0):
            raise AirflowException("Invalid connection or query settings")
        if self.op.startswith("cmp") and (self.conn_id2 is None or self.sql2 is None):
            raise AirflowException(
                "Invalid connection or query settings for comparison operator"
            )

        try:
            hook = get_hook(self.conn_id1)
            self.conn1 = hook.get_conn()

            if self.conn_id2:
                hook2 = get_hook(self.conn_id2)
                self.conn2 = hook2.get_conn()

            if self.op == "assertTrue":
                self.assertTrue(self.conn1, self.sql1)
            elif self.op == "assertAll":
                self.assertAll(self.conn1, self.test_suite)
            elif self.op == "cmpEquals":
                self.cmpEquals(self.conn1, self.sql1, self.conn2, self.sql2)
            elif self.op == "cmpGreaterThan":
                self.cmpGreaterThan(self.conn1, self.sql1, self.conn2, self.sql2)
            elif self.op == "cmpDelta":
                self.cmpDelta(
                    self.conn1, self.sql1, self.conn2, self.sql2, self.delta_perc
                )
            else:
                raise AirflowException(
                    "Unknown operation type for EtlValidationOperator"
                )

        except Exception as e:
            print("etl_validation failed for {}: {}".format(self.task_id, str(e)))
            raise

        finally:
            if hasattr(self, "conn1"):
                self.conn1.close()
            if hasattr(self, "conn2"):
                self.conn2.close()

    def assertTrue(self, conn, sql):
        """
        Executes the sql and compares query result equals 1 (True) to raise validation exception
        (multiple columns in select must all eval to bool)
        Sql expression should evaluate to bool result and should check for existence of failure conditions to be asserted here
        example - select count(1) = 0 : results in assertTrue to raise exception when the underlying table has zero records
        :param conn:                db connection obj
        :type conn:                 mysql db connection

        :param sql:                 sql statement
        :type sql:                  string
        """
        for row in self.get_query_results(conn, sql):
            self.log.info(
                "assertTrue:: task: {}, value: {}".format(self.task_id, str(row))
            )
            retVals = [int(v) for v in row]  # presto returns True/False, convert to 1/0

            if not all(
                retVals
            ):  # if any of the col expressions evaluates to False, then exception condition met
                raise AirflowException(
                    "EtlValidation assertionError: {} from query {}".format(
                        self.task_id, sql
                    )
                )

    def assertAll(self, conn, test_suite):
        """
        Executes the sql's in test_suite and compares query result equals 1 (True) for each sql, else raise validation exception
        Sql expression should evaluate to bool result and should check for validation failure conditions to be asserted here
        :param conn:                db connection obj
        :type conn:                 mysql db connection

        :param test_suite:          group of test cases with corresponding assertion queries
        :type test_suite:           dict: {'test_cases': [ {'name': 'n1', 'sql': 'sql1'}, {'name': 'n2', 'sql': 'sql2'}] }
        """
        if "test_cases" not in test_suite or len(test_suite["test_cases"]) == 0:
            raise Exception("assertAll:: Invalid test_suite format")

        for tc in test_suite["test_cases"]:
            tc_name, sql = tc["name"], tc["sql"]

            for row in self.get_query_results(conn, sql):
                self.log.info(
                    "assertAll:: task: {}, tc_name: {}, value: {}".format(
                        self.task_id, tc_name, str(row)
                    )
                )
                retVals = [int(v) for v in row]

                if any([val is False for val in retVals]):
                    raise AirflowException(
                        "EtlValidation assertionError: {} from query {}".format(
                            tc_name, sql
                        )
                    )

    def cmpEquals(self, conn1, sql1, conn2, sql2):
        """
        Compare difference in results returned between the two given queries. Raise exception if results dont match
        """
        for row in self.get_query_results(conn1, sql1):
            res1 = row[0]
        for row in self.get_query_results(conn2, sql2):
            res2 = row[0]
        self.log.info(
            "cmpEquals:: task: {}, value1: {}, value2: {}".format(
                self.task_id, str(res1), str(res2)
            )
        )

        if res1 != res2:
            raise AirflowException(
                "EtlValidation cmpEqualsError: query {}".format(sql1 + "<>" + sql2)
            )

    def cmpGreaterThan(self, conn1, sql1, conn2, sql2):
        """
        Compare results from sql1 greater-than results from sql2. Raise exception if not
        """
        for row in self.get_query_results(conn1, sql1):
            res1 = row[0]
        for row in self.get_query_results(conn2, sql2):
            res2 = row[0]
        self.log.info(
            "cmpGreaterThan:: task: {}, value1: {}, value2: {}".format(
                self.task_id, str(res1), str(res2)
            )
        )

        if res1 <= res2:
            raise AirflowException(
                "EtlValidation cmpGreaterThanError: query {}".format(sql1 + "<=" + sql2)
            )

    def cmpDelta(self, conn1, sql1, conn2, sql2, delta_perc):
        """
        Compare difference in results returned between the two given queries. Raise exception if deviation is > delta_perc
        """
        for row in self.get_query_results(conn1, sql1):
            res1 = row[0]
        for row in self.get_query_results(conn2, sql2):
            res2 = row[0]
        self.log.info(
            "cmpDelta:: task: {}, value1: {}, value2: {}".format(
                self.task_id, str(res1), str(res2)
            )
        )

        try:
            if (abs(res1 - res2) / res1) * 100 > delta_perc:
                raise AirflowException(
                    f"""EtlValidation cmpDeltaError: Query results delta greater than {delta_perc}%
Query 1:
{sql1}
Result: {res1}
<>
Query 2:
{sql2}
Result: {res2}"""
                )
        except ZeroDivisionError:
            if res1 != res2:
                raise AirflowException(
                    f"Ran into ZeroDivisionError where res1 = 0 and res2 = {res2}"
                )

    def get_query_results(self, conn, sql):
        """
        Execute given query against given connection and return the result rows
        :param conn:        db connection
        :param sql:         sql statement to exec
        """

        cur = conn.cursor()
        cur.execute(sql)
        yield from cur.fetchall()


class EtlValidationPlugin(AirflowPlugin):
    name = "etl_validation_plugin"
    operators = [EtlValidationOperator]
