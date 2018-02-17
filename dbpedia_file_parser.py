import rdflib
import os
import logging
from database_query_handler import DBHandler

"""

Parses the provided persondata_en.tql using RDFlib and stores it in a connected Postgres database.

"""


def build_tql_file_generator(filepath):

    """
    
    Provides a generator for pulling DBpedia tuples (subject, predicate, value) using the RDFlib; TQL format is a 
    subset of 'Notation 3' syntax, so apparently this package can parse that directly.
    
    Given that the size of tql files is unbounded, this method returns a generator so it isn't necessary to load
    the entire dataset at once.
    
    :param filepath: absolute path to the tql file
    :return: generator providing (subject, predict, value) tuples for consumption
    """

    with open(filepath, 'rU') as f:

        for line in f:
            # remove new lines from generator output
            yield line.strip()


def plain_subject_predicate(line):

    """
    
    Converts spo format into something approximately human readable.
    
    :param line: 
    :return: 
    """

    if '22-rdf-syntax-ns#' in line:
        return line.split('/')[-1].split('#')[-1]
    else:
        return line.split('/')[-1]


def construct_sql_db(schema, schema_path, db_tql_file_path, overwrite_db=False):

    """
    
    Handles the initial construction of the local DBpedia SQL database. Throws an assertion
    error if you attempt to destroy and replace the DB without setting overwrite_db to True.
    
    This method takes quite a bit of time (~hours) to complete.
    
    :param schema_path:
    :param db_tql_file_path: 
    :param overwrite_db:
    :return: 
    """

    db_handler = DBHandler()

    if not overwrite_db and db_handler.schema_exists():
        raise AssertionError('Set overwrite_db flag to True to overwrite an existing DB instance.')

    db_handler.build_table_schema(schema, schema_path)
    generator = build_tql_file_generator(db_tql_file_path)

    counter = 0

    for line in generator:

        # this is much slower than having a single graph read once
        # however, this requires much less memory given that
        # rdflib does not support reading from streams
        g = rdflib.graph.ConjunctiveGraph()
        g.parse(data=line, format='nquads')

        for (subject, predicate, db_object) in g:

            subject = plain_subject_predicate(subject)
            predicate = plain_subject_predicate(predicate)
            db_object = plain_subject_predicate(db_object)

            db_handler.insert_spo_tuple((subject, predicate, db_object))

        counter += 1

    db_handler.build_indices()

    db_handler.commit()

    return None

if __name__ == '__main__':

    db_handler = DBHandler()

    # db_handler.build_table_schema('dbpedia', os.path.join(os.getcwd(), 'sql', 'dbpedia_schema.sql'))

    # generator = build_tql_file_generator(os.path.join(os.getcwd(), 'dbpedia', 'persondata_en.tql'))

    results = db_handler.get_person_metadata('Buddy', use_exact_match=False)

    for result in results:
        print result

