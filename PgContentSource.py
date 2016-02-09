__author__ = 'mcardle'
"""
Implementation of the PostgreSQL ContentSrc database JDBC connection

/Library/PostgreSQL/pgJDBC/postgresql-9.4-1200.jdbc4.jar
myuser, myuser
jdbc:postgresql:contentSrc
org.postgresql.Driver

"""
import DbmsCore as dbcore


class PgContentSource(dbcore.JdbcConnection):
    """
    A concrete database connection for postgreSQL
    Assumes PostreSQL driver in JAVA PATH
    TODO: Fix to add jar_location to avoid assumption and make generic
    """
    def __init__(self):
        driver = 'org.postgresql.Driver'
        driver_args = ['jdbc:postgresql:contentSrc', 'myuser','myuser']
        super(PgContentSource, self).__init__(driver, driver_args)

