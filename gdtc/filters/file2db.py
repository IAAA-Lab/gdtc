import aux.db as gdtcdb
import filters.basefilters as basefilters


class ExecSQL(basefilters.File2DBFilter):
    def run(self):
        db = gdtcdb.Db(self.params['output_db_host'],
                       self.params['output_db_port'],
                       self.params['output_db_user'],
                       self.params['output_db_password'],
                       self.params['output_db_database'])

        with open(self.params['input_path'], "r") as file:
            sql = file.read()

        db.execute_query(sql)

