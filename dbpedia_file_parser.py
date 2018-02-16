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


if __name__ == '__main__':

    db_handler = DBHandler()

    db_handler.build_table_schema('dbpedia', os.path.join(os.getcwd(), 'sql', 'dbpedia_schema.sql'))

    generator = build_tql_file_generator(os.path.join(os.getcwd(), 'dbpedia', 'persondata_en.tql'))
    g = rdflib.graph.ConjunctiveGraph()
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

    db_handler.commit()