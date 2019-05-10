import logging

import gdtc.filters.basefilters as basefilters
import gdtc.aux.db as gdtcdb

# TODO: Create random table name generator

class RowFilter(basefilters.DB2DBFilter):
    def run(self):

        logging.debug(f' Executing RowFilter filter with params: {self.params}')

        sql = f'''
                  DROP TABLE IF EXISTS \"{self.params["output_db_table"]}\";
                  CREATE TABLE \"{self.params["output_db_table"]}\" AS (
                      SELECT * FROM \"{self.params["input_db_table"]}\" WHERE {self.params["where_clause"]}
                  )
                '''

        logging.debug(f' SQL to execute: {sql}')        

        db = gdtcdb.Db(*self.get_output_connection().values())
        db.execute_query(sql)