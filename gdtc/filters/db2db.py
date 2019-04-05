import filters.basefilters as basefilters
import aux.db as gdtcdb

class RowFilter(basefilters.DB2DBFilter):
    def run(self):
        sql = f'''
                  DROP TABLE IF EXISTS {self.params["db_table"]}_2;
                  CREATE TABLE {self.params["db_table"]}_2 AS (
                      SELECT * FROM {self.params["db_table"]} WHERE {self.params["where_clause"]}
                  )
                '''
                
        db = gdtcdb.Db(*self.get_output().values())
        db.execute_query(sql)