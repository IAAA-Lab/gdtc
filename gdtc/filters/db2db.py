import logging
import psycopg2

import gdtc.filters.basefilters as basefilters
import gdtc.aux.db as gdtcdb


class RowFilter(basefilters.DB2DBFilter):
    def run(self):

        logging.debug(f' Executing RowFilter filter with params: {self.params}')
        # FIXME: sql queries should not be created by formatting strings
        # see: http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries
        sql = f'''
                  DROP TABLE IF EXISTS \"{self.params["output_db_table"]}\";
                  CREATE TABLE \"{self.params["output_db_table"]}\" AS (
                      SELECT * FROM \"{self.params["input_db_table"]}\" WHERE {self.params["where_clause"]}
                  )
                '''

        logging.debug(f' SQL to execute: {sql}')        

        db = gdtcdb.Db(*self.get_output_connection().values())
        try:
            db.execute_query(sql)
        except psycopg2.Error as e:
            msg = f' Error executing query: {e}'
            logging.error(msg)
            raise RuntimeError(msg)  # If we fail, run must end with an error
        finally:
            db.close_connection()