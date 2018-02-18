import os
import sys
from database_query_handler import DBHandler
import dbpedia_file_parser
from collections import defaultdict
import time


def construct_database(postgres_username,
                       postgres_password):

    """
    
    Attempts to reconstruct the local dbpedia using Postgres.
    
    :param postgres_username: 
    :param postgres_password: 
    :return: 
    """

    current_location = os.path.realpath(__file__)
    path = os.path.split(current_location)[0]

    print 'Working on building a local DBpedia database...'

    try:
        dbpedia_file_parser.driver(path,
                                   postgres_username,
                                   postgres_password)

        print 'Database construction finished successfully!'
    except Exception as e:
        print e.message
        print 'This may be caused be insufficient access rights for the ' \
              'local postgres user account used to create the DB, or a missing postgres installation.'


def example_queries():

    try:
        db_handler = DBHandler()
    except:
        raise

    # extract person metadata
    person_of_interest = 'Tokuko Takagi'
    print 'Example of querying data on one person: %s' % person_of_interest
    results = db_handler.get_person_metadata(person_of_interest)

    for (s, p, o) in results:
        print '%s, %s: %s' % (s, p, o)

    # get most common surname
    results = db_handler.get_tuples_by_predicate('givenname')

    counter = defaultdict(int)
    for (_, _, o) in results:
        counter[o.upper()] += 1

    print '\nMost common given name in DBpedia:\n%s' % max(counter, key=counter.get)

    # get earliest birthday
    results = db_handler.get_tuples_by_predicate('birthdate')
    earliest_birthday_tuple = None

    for (s, _, o) in results:

        # standardize tuple first; Wikipedia dates aren't really proper for datetime, but can use struct_time
        # instead
        year_sign = 1
        if o[0] == '-':
            year_sign = -1
            o = o[1:]

        date_tokens = o.split('-')
        year = int(date_tokens[0]) * year_sign
        month = int(date_tokens[1])
        day = int(date_tokens[2])

        date_object = time.struct_time((year,
                                       month,
                                       day,
                                        0,
                                        0,
                                        0,
                                        0,
                                        1,
                                        0))

        if earliest_birthday_tuple is None:
            earliest_birthday_tuple = (s, date_object)
        else:
            # comparison operators are overridden here
            if date_object < earliest_birthday_tuple[1]:
                earliest_birthday_tuple = (s, date_object)

    print '\nEarliest birth date in DBpedia belongs to:\n%s at %s-%s-%s (Y/M/D)' % (earliest_birthday_tuple[0],
                                                                                    earliest_birthday_tuple[1][0],
                                                                                    earliest_birthday_tuple[1][1],
                                                                                    earliest_birthday_tuple[1][2])

    # five most common words in db used to describe people
    # note that this includes common stop words and conjunctions
    # can be removed if you have a stop word list handy (i.e. from NTLK)
    results = db_handler.get_tuples_by_predicate('description')

    counter = defaultdict(int)
    for (_, _, o) in results:
        tokens = [x.upper() for x in o.split(' ')]
        for t in tokens:
            counter[t] += 1

    sorted_description_tokens = sorted([(x, y) for x, y in counter.iteritems()],
                                       key=lambda val: val[1],
                                       reverse=True)

    print '\nMost common words used to describe people in DBpedia:'
    for (word, count) in sorted_description_tokens[0:5]:
        print '%s: %i' % (word, count)


if __name__ == '__main__':

    if len(sys.argv) == 2 and sys.argv[1] == 'example':
        # assume DB has been constructed, run example data analysis
        example_queries()
    elif len(sys.argv) == 2 and sys.argv[1] == 'rebuild':
        # construct database
        pg_username = None
        pg_password = None
        construct_database(pg_username, pg_password)
    elif len(sys.argv) == 4 and sys.argv[1] == 'rebuild':
        # construct database
        pg_username = sys.argv[1]
        pg_password = sys.argv[2]
        construct_database(pg_username, pg_password)
    elif len(sys.argv) == 1:
        print "Usage: 'python driver.py example' generates an example of output data"
        print "'python driver.py rebuild' (re)builds a local DBpedia database " \
              "using the local postgres install but assumes you have created the database 'dbpedia' already"
        print "'python driver.py rebuild PG_USER_NAME PG_PASSWORD' (re)builds a local DBpedia database " \
              "using the local postgres install"
        print "'python driver.py' displays this message."
    else:
        print 'Unknown number of arguments.'
