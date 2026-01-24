from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password" #TODO env variable?
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432


class PgsqlToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres"
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
    ):
        uri = dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, db_name
        )

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            connection_uri=uri,
            autocommit=autocommit,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        connection_uri: str,
        autocommit: bool,
    ):
        super().__init__(connection, result_collector=collector)
        self._connection_uri = connection_uri
        self.autocommit = autocommit

        self.current_branch = "main"


    def get_uri_for_db_setup(self) -> str:
        """Returns the connection URI for database setup operations (e.g., PGSQL)."""
        return self._connection_uri

    def _create_branch_impl(self, branch_name: str, parent_id: str) -> None:
        cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_id} STATEGY = FILE_COPY"
        super().execute_sql(cmd)
        self.current_branch = branch_name

    def _connect_branch_impl(self, branch_name: str) -> None:
        cmd = f"\\c {branch_name}"
        super().execute_sql(cmd)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self.current_branch, 0) if res else None # branch_id not implemented
