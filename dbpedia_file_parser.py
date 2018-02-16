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

    if '22-rdf-syntax-ns#' in line:
        return line.split('/')[-1].split('#')[-1]
    else:
        return line.split('/')[-1]


if __name__ == '__main__':

    generator = build_tql_file_generator(os.path.join(os.getcwd(), 'dbpedia', 'persondata_en.tql'))
    g = rdflib.graph.ConjunctiveGraph()

    for line in generator:

        g = rdflib.graph.ConjunctiveGraph()
        g.parse(data=line, format='nquads')

        for (subject, predicate, object) in g:

            subject = plain_subject_predicate(subject)
            predicate = plain_subject_predicate(predicate)
            object = plain_subject_predicate(object)