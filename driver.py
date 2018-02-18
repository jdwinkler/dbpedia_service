import os
import sys
from database_query_handler import DBHandler
from collections import defaultdict
import time
import nltk


def construct_database(pg_username, pg_password):

    try:
        db_handler = DBHandler(postgres_username=pg_username,
                               postgres_password=pg_password)
    except:
        raise

    return None


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
            # comparison operators are overriden here
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

    sorted_description_tokens = sorted([(x, y) for x,y in counter.iteritems()],
                                       key=lambda val: val[1],
                                       reverse=True)

    print '\nMost common words used to describe people in DBpedia:'
    for (word, count) in sorted_description_tokens[0:5]:
        print '%s: %i' % (word, count)


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        # assume DB has been constructed, run example data analysis
        example_queries()
    elif len(sys.argv) == 3:
        # construct database
        pg_username = sys.argv[1]
        pg_password = sys.argv[2]
        construct_database(pg_username, pg_password)
    else:
        print('Unknown number of arguments.')