import logging
import psycopg2
from psycopg2 import sql

import gdtc.filters.basefilters as basefilters
import gdtc.aux.db as gdtcdb


class RowFilter(basefilters.DBs2DBsFilter):
    def run(self):
        logging.debug(f' Executing RowFilter filter with params: {self.params}')

        query = sql.Composed([sql.SQL("DROP TABLE IF EXISTS "),
                                sql.Identifier(self.get_outputs()[0]['db_table']),
                                sql.SQL("; CREATE TABLE "),
                                sql.Identifier(self.get_outputs()[0]['db_table']),
                                sql.SQL(" AS ( SELECT * FROM "),
                                sql.Identifier(self.get_inputs()[0]['db_table']),
                                sql.SQL(" WHERE "),
                                sql.SQL(self.params["where_clause"]),
                                sql.SQL(" );")
                            ])
        
        db = gdtcdb.db_factory(self.get_outputs()[0])
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