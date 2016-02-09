__author__ = 'a.j.mcardle'
# coding=utf-8
""" This is a base class for use with RDBMS drivers ( JDBC ).
    Child classes must provide the actual strings to connect to the appropriate data source
    The child class must use the 'with' idiom to actually connect:

    with ChildClass.ChildDB() as connection:
        ...do stuff with connection
    //Once out of scope connection will be automatically closed
"""
import jaydebeapi


class JdbcConnection(object):
    def __init__(self, driver_class_name, driver_args):
        self.driver_class_name = driver_class_name
        self.driver_args = driver_args
        self.__connection = None

    def __enter__(self):
        self.__connection = jaydebeapi.connect(self.driver_class_name, self.driver_args)
        return self.__connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__close_connection()

    @property
    def connection(self):
        return self.__connection

    def close_connection(self):
        try:
            self.__connection.close()
        except:
            pass
        finally:
            self.__connection = None

    __close_connection = close_connection
