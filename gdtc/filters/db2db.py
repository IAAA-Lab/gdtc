import logging
import psycopg2
from psycopg2 import sql

import gdtc.filters.basefilters as basefilters
import gdtc.aux.db as gdtcdb


class RowFilter(basefilters.DB2DBFilter):
    def run(self):

        logging.debug(f' Executing RowFilter filter with params: {self.params}')
        # FIXME: sql queries should not be created by formatting strings
        # see: http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        query = sql.Composed([sql.SQL("DROP TABLE IF EXISTS "),
                                sql.Identifier(self.params["output_db_table"]),
                                sql.SQL("; CREATE TABLE "),
                                sql.Identifier(self.params["output_db_table"]),
                                sql.SQL(" AS ( SELECT * FROM "),
                                sql.Identifier(self.params["input_db_table"]),
                                sql.SQL(" WHERE "),
                                sql.SQL(self.params["where_clause"]),
                                sql.SQL(" );")
                            ])
        
        db = gdtcdb.Db(*self.get_output_connection().values())

        str_query = query.as_string(db.get_connection())
        logging.debug(f' SQL to execute: {str_query}')        

        try:
            db.execute_query(str_query)
        except psycopg2.Error as e:
            msg = f' Error executing query: {e}'
            logging.error(msg)
            raise RuntimeError(msg)  # If we fail, run must end with an error
        finally:
            db.close_connection()