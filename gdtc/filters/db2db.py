import filters.basefilters as basefilters
import aux.db as gdtcdb

# TODO: Create random table name generator

class RowFilter(basefilters.DB2DBFilter):
    def run(self):
        sql = f'''
                  DROP TABLE IF EXISTS \"{self.params["db_table_out"]}\";
                  CREATE TABLE \"{self.params["db_table_out"]}\" AS (
                      SELECT * FROM \"{self.params["db_table"]}\" WHERE {self.params["where_clause"]}
                  )
                '''

        db = gdtcdb.Db(*self.get_output().values())
        db.execute_query(sql)