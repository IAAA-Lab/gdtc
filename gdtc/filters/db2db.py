import filters.basefilters as basefilters

class RowFilter(basefilters.DB2DBFilter):
    def run(self):
        print(self.params["db_table"])    