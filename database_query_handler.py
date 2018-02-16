import psycopg2
import psycopg2.extras
import os
from collections import defaultdict

class DBHandler:

    def __init__(self):

        # ordinarily you would get these from some secret store
        # e.g. heroku as a specific url that you parse to get both
        # or os.environ storage (like those used for API keys and the like)
        user_name = 'dbpedia_app'
        password = 'dummy_password'

        connection = psycopg2.connect("dbname='postgres' user='%s' host='localhost' password='%s'"
                                      % ('postgres', 'password'))
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'dbpedia'")
        not_exists_row = cursor.fetchone()
        not_exists = not_exists_row[0]
        if not_exists:
            cursor.execute('CREATE USER %s PASSWORD %s' % (user_name, password))
            cursor.execute('CREATE DATABASE dbpedia OWNER %s' % (user_name,))

        connection.close()

        connection = psycopg2.connect("dbname='dbpedia' user='%s' host='localhost' password='%s'"
                                      % (user_name, password))

        connection.set_session(readonly=True)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        self.connection = connection
        self.cursor = cursor

    def __del__(self):

        self.connection.close()
