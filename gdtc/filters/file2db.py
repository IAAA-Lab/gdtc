import aux.db as gdtcdb
import filters.basefilters as basefilters


class ExecSQLFile(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(*self.get_output_connection().values())

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)

