import filters.basefilters as basefilters
import aux.db as gdtcdb

# TODO: Create random table name generator

class RowFilter(basefilters.DB2DBFilter):
    def run(self):
        sql = f'''
                  DROP TABLE IF EXISTS \"{self.params["output_db_table"]}\";
                  CREATE TABLE \"{self.params["output_db_table"]}\" AS (
                      SELECT * FROM \"{self.params["input_db_table"]}\" WHERE {self.params["where_clause"]}
                  )
                '''

        db = gdtcdb.Db(*self.get_output_connection().values())
        db.execute_query(sql)