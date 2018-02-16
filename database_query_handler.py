import psycopg2
import psycopg2.extras


class DBHandler:

    """
    
    Handles I/O concerning the database to hide its implementation from client services.
    
    """

    def __init__(self):

        # ordinarily you would get these from some secret store
        # e.g. heroku as a specific url that you parse to get both
        # or os.environ storage (like those used for API keys and the like)
        user_name = 'dbpedia_app'
        password = 'dummy_password'

        # check to see if the db exists locally, create it if necessary
        connection = psycopg2.connect("dbname='postgres' user='%s' "
                                      "host='localhost' password='%s'"
                                      % ('postgres', 'password'))
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'dbpedia'")
        not_exists_row = cursor.fetchone()
        not_exists = not_exists_row[0]
        if not_exists:
            cursor.execute("CREATE USER %s PASSWORD '%s'" % (user_name, password))
            cursor.execute('CREATE DATABASE dbpedia OWNER %s' % (user_name,))

        connection.close()

        connection = psycopg2.connect("dbname='dbpedia' user='%s' host='localhost' password='%s'"
                                      % (user_name, password))

        self.connection = connection

    def __del__(self):

        self.connection.close()

    def commit(self):

        self.connection.commit()

    def build_table_schema(self, schema_name, schema_file_path):

        """
        
        Loads the dbpedia schema used for supporting downstream analysis. If the schema already exists, it is
        dropped (deleted) and recreated.
        
        :param schema_name: 
        :param schema_file_path: 
        :return: 
        """

        # do not call with user input given the manual query construction here

        with self.connection.cursor() as cursor:

            cursor.execute('DROP SCHEMA IF EXISTS %s CASCADE' % schema_name)
            schema_file = open(schema_file_path, 'rU').read()
            cursor.execute(schema_file)

    def insert_spo_tuple(self, spo_tuple):

        """
        
        Handles the insertion of spo tuples into the db. Workflow:
        
        Attempt to find the subject table entry corresponding to your subject. If found, use that ID for
        inserting your po values. Otherwise, insert your subject into the subject table and use that ID
        instead. The resulting id, predicate, object tuple is then inserted into the predicate_object table.
        
        :param spo_tuple: 
        :return: 
        """

        (subject, predicate, db_object) = spo_tuple

        with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

            cursor.execute('select subject_id from dbpedia.subjects '
                           'where name = %s', (subject,))

            results = cursor.fetchone()

            if results is None or len(results) == 0:

                cursor.execute('INSERT INTO dbpedia.subjects (name) VALUES (%s) '
                               'returning subject_id', (subject,))
                results = cursor.fetchone()

            id = results['subject_id']

            # now we have the correct id in either case, insert the values into the db

            cursor.execute('INSERT INTO dbpedia.predicate_object (subject_id, predicate, object) '
                           'VALUES (%s, %s, %s)', (id, predicate, db_object))
