import logging
import psycopg2
from psycopg2 import sql

from gdtc.filters.basefilters import DBs2DBsFilter
import gdtc.aux.db

class ContainedIn(DBs2DBsFilter):
    """
    Takes 2 input tables and creates another table with those geometries from table 1 contained
    in those from table 2.
    """
    def run(self):
        logging.debug(f' Executing ContainedIn filter with params: {self.params}')

        if 'geom1name' not in self.get_params():
            geom1name = 'geom'
        else:
            geom1name = self.get_params()['geom1name']

        if 'geom2name' not in self.get_params():
            geom2name = 'geom'
        else:
            geom2name = self.get_params()['geom2name']

        # Add addtional constraints in WHERE.
        # Select colums?

        query = sql.SQL(
            """
            CREATE TEMPORARY TABLE {0} AS
            SELECT *
            FROM {1} as contained, {2} as container
            WHERE ST_Contains(contained.{3}, container.{4})
            ;
            """).format(sql.Identifier(self.get_outputs()[0]['db_table']),
                        sql.Identifier(self.get_inputs()[0]['db_table']),
                        sql.Identifier(self.get_inputs()[1]['db_table']),
                        sql.Identifier(geom1name),
                        sql.Identifier(geom2name))

        logging.debug(f' SQL to execute: {query}')

        db = gdtc.aux.db.Db(*self.get_output_connection().values())

        try:
            db.execute_query(sql)
        except psycopg2.Error as e:
            msg = f' Error executing query: {e}'
            logging.error(msg)
            raise RuntimeError(msg)  # If we fail, run must end with an error
        finally:
            db.close_connection()

        logging.debug(f' ContainedIn finished')
