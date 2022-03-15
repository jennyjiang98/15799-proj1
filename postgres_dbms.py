# adapted from hyrise: https://github.com/hyrise/index_selection_evaluation
import logging
import re
import psycopg2

from database_connector import DatabaseConnector


class PostgresDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit)
        self.db_system = "postgres"
        self._connection = None

        if not self.db_name:
            self.db_name = "project1db"
        self.create_connection()

        self.set_random_seed()
        self.enable_simulation()
        logging.debug("Postgres connector created: {}".format(db_name))

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = psycopg2.connect("dbname={} user=project1user password=project1pass host=127.0.0.1".format(self.db_name))
        self._connection.autocommit = self.autocommit
        self._cursor = self._connection.cursor()

    def enable_simulation(self):
        self.exec_only("CREATE EXTENSION IF NOT EXISTS hypopg")
        self.commit()

    def database_names(self):
        result = self.exec_fetch("select datname from pg_database", False)
        return [x[0] for x in result]

    def create_database(self, database_name):
        self.exec_only("create database {}".format(database_name))
        logging.info("Database {} created".format(database_name))

    def import_data(self, table, path, delimiter="|"):
        with open(path, "r") as file:
            self._cursor.copy_from(file, table, sep=delimiter, null="")

    def indexes_size(self):
        # Returns size in bytes
        statement = (
            "select sum(pg_indexes_size(table_name::text)) from "
            "(select table_name from information_schema.tables "
            "where table_schema='public') as all_tables"
        )
        result = self.exec_fetch(statement)
        return result[0]

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        logging.info("Postgres: Run `analyze`")
        self.commit()
        self._connection.autocommit = True
        self.exec_only("analyze")
        self._connection.autocommit = self.autocommit

    def set_random_seed(self, value=0.17):
        logging.info(f"Postgres: Set random seed `SELECT setseed({value})`")
        self.exec_only(f"SELECT setseed({value})")

    def supports_index_simulation(self):
        if self.db_system == "postgres":
            return True
        return False

    def _simulate_index(self, cols, table_name):
        names = ",".join(cols)
        statement = (
            "select * from hypopg_create_index( "
            f"'create index on {table_name} "
            f"({names})')"
        )
        result = self.exec_fetch(statement)
        return result

    def _drop_simulated_index(self, oid):
        statement = f"select * from hypopg_drop_index({oid})"
        result = self.exec_fetch(statement)

        assert result[0] is True, f"Could not drop simulated index with oid = {oid}."

    def create_index(self, cols, table_name):
        names = ",".join(cols)
        statement = (
            f"create index {'index_' + table_name +'_'+ '_'.join(cols)} "
            f"on {table_name} ({names})"
        )
        self.exec_only(statement)
        # size = self.exec_fetch(
        #     f"select relpages from pg_class c " f"where c.relname = '{index.index_idx()}'"
        # )
        # size = size[0]
        # index.estimated_size = size * 8 * 1024

    def drop_all_indexes(self):
        logging.info("Dropping indexes")
        stmt = "select indexname from pg_indexes where schemaname='public'"
        indexes = self.exec_fetch(stmt, one=False)
        for index in indexes:
            index_name = index[0]
            if 'pkey' in index_name:
                continue
            drop_stmt = "drop index {}".format(index_name)
            print("Dropping index {}".format(index_name))
            self.exec_only(drop_stmt)

    # PostgreSQL expects the timeout in milliseconds
    def exec_query(self, query, timeout=None, cost_evaluation=False):
        # Committing to not lose indexes after timeout
        if not cost_evaluation:
            self._connection.commit()
        if timeout:
            set_timeout = f"set statement_timeout={timeout}"
            self.exec_only(set_timeout)
        statement = f"explain (analyze, buffers, format json) {query}"
        try:
            plan = self.exec_fetch(statement, one=True)[0][0]["Plan"]
            result = plan["Actual Total Time"], plan
        except Exception as e:
            logging.error(f"{query.nr}, {e}")
            self._connection.rollback()
            result = None, self._get_plan(query)
        # Disable timeout
        self._cursor.execute("set statement_timeout = 0")
        return result

    def is_col_varchar(self, col_name, tab_name):
        statement = "SELECT data_type FROM information_schema.columns WHERE table_name = '{}' and column_name = '{}';".format(tab_name, col_name)
        type_name = self.exec_fetch(statement, one=True)[0]
        return 'char' in type_name

    def _get_cost(self, query):
        query_plan = self._get_plan(query)
        total_cost = query_plan["Total Cost"]
        return total_cost

    def _get_plan(self, query):
        statement = f"explain (format json) {query}"
        query_plan = self.exec_fetch(statement)[0][0]["Plan"]
        return query_plan

    def try_exec(self, statement):
        try:
            self._cursor.execute(statement)
            return True
        except Exception as e:
            logging.error(f"{statement}, {e}")
            self._connection.rollback()
            return False

