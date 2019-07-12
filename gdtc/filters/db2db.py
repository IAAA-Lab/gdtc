import logging
import psycopg2
from psycopg2 import sql

import gdtc.filters.basefilters as basefilters
import gdtc.aux.db as gdtcdb
from filters.basefilters import DBs2DBsFilter


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


class ClipRasterWithSHP(basefilters.DBs2DBsFilter):

    def run(self):
        query = sql.Composed([
                        sql.SQL("DROP TABLE IF EXISTS "),
                        sql.Identifier(self.get_outputs()[0]['db_table']),
                        sql.SQL("; "),
                        sql.SQL("CREATE TABLE "),
                        sql.Identifier(self.get_outputs()[0]['db_table']),
                        sql.SQL(" (rast raster); INSERT INTO "),
                        sql.Identifier(self.get_outputs()[0]['db_table']),
                        sql.SQL("(rast) VALUES (( SELECT ST_Clip (rast, (SELECT "),
                        sql.Identifier(self.params["geom"]),
                        sql.SQL(" FROM "),
                        sql.Identifier(self.get_inputs()[0]['db_table']),
                        sql.SQL(" WHERE ogc_fid = "),
                        sql.SQL(self.params["ogc_fid"]),
                        sql.SQL("), true) FROM "),
                        sql.Identifier(self.get_inputs()[1]['db_table']),
                        sql.SQL(" WHERE rid = "),
                        sql.SQL(self.params["rid"]),
                        sql.SQL(" ));")
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


class ContainedIn(DBs2DBsFilter):
    """
    Takes 2 input tables and creates another table with those geometries from table 1 contained
    in those from table 2.
    """
    def run(self):
        logging.debug(f' Executing ContainedIn filter with params: {self.params}')

        if 'geom1name' not in self.get_params():
            geom1name = 'geom'
        else:
            geom1name = self.get_params()['geom1name']

        if 'geom2name' not in self.get_params():
            geom2name = 'geom'
        else:
            geom2name = self.get_params()['geom2name']

        # Add addtional constraints in WHERE.
        # Select colums?

        query = sql.SQL(
            """
            CREATE TEMPORARY TABLE {0} AS
            SELECT *
            FROM {1} as contained, {2} as container
            WHERE ST_Contains(contained.{3}, container.{4})
            ;
            """).format(sql.Identifier(self.get_outputs()[0]['db_table']),
                        sql.Identifier(self.get_inputs()[0]['db_table']),
                        sql.Identifier(self.get_inputs()[1]['db_table']),
                        sql.Identifier(geom1name),
                        sql.Identifier(geom2name))

        logging.debug(f' SQL to execute: {query}')

        db = gdtcdb.Db(*self.get_output_connection().values())

        try:
            db.execute_query(sql)
        except psycopg2.Error as e:
            msg = f' Error executing query: {e}'
            logging.error(msg)
            raise RuntimeError(msg)  # If we fail, run must end with an error
        finally:
            db.close_connection()

        logging.debug(f' ContainedIn finished')