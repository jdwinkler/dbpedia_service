# dbpedia_service

Tool for storing, accessing, and querying the DBPedia People database. Provides the basic tools for parsing the database into a Postgres database and querying for basic information.

## Getting Started

Clone the repository and make sure the following packages are installed:

Psycopg2 (2.7)<br>
RDFlib (4.2.2)

The CLI is as follows:

'python driver.py' displays help information.<br>
'python driver.py rebuild' assumes a database named dbpedia, owned by dbpedia_app with the password 'dummy_password', already exists and then (re)converts the DBpedia dataset into a postgres database<br>
'python driver.py rebuild PG_USERNAME PG_PASSWORD' will automatically create the database named dbpedia, owned by dbpedia_app, and then (re)converts the DBpedia dataset into a postgres database assuming PG_USERNAME has access rights to create databases.

Note that the initial database construction is quite slow, but not very memory intensive; subject-predicate-object tuples are essentially "streamed" individually into the DB to avoid reconstructing the entire DBpedia database into memory at once.

'python driver.py' example will automatically generate the following example data output:

Example of querying data on one person: Tokuko Takagi<br>
Tokuko_Takagi, birthPlace: Tokyo<br>
Tokuko_Takagi, description: Japanese actor and dancer<br>
Tokuko_Takagi, birthDate: 1891-01-01<br>
Tokuko_Takagi, name: Tokuko Takagi<br>
Tokuko_Takagi, gender: female<br>
Tokuko_Takagi, deathDate: 1919-01-01<br>
Tokuko_Takagi, type: Person<br>

Most common given name in DBpedia:<br>
JOHN<br>

Earliest birth date in DBpedia belongs to:<br>
Koelbjerg_Woman at -7999-1-1 (Y/M/D)<br>

Most common words used to describe people in DBpedia:<br>
AMERICAN: 278219<br>
POLITICIAN: 136467<br>
PLAYER: 136414<br>
AND: 135920<br>
FOOTBALLER: 122983<br>

### Prerequisites

Postgres (9.5 or higher) must be installed as well on your local machine, and you must have access to a user account with enough access rights to create and delete databases. Alternatively, you can manually create a database name 'dbpedia' (without quotes) with username: dbpedia_app and password: dummy_password .

You will also need to decompress the DBpedia people database (obtainable from [here](http://downloads.dbpedia.org/2016-10/core-i18n/en/persondata_en.tql.bz2)) into the dbpedia folder within the repository. Git Large File Storage can also be used to store the file potentially, but on free accounts, the size of the uncompressed file is limited to 1 GB compared to the 1.7 GB file. 

## Running the tests

Here are aspects of the program that should be tested:

1. Integration with postgres and what happens when the local install is missing, inaccessible, or the user passes in credentials that do not have the right to create or drop databases
2. Various parsing failure modes, such as how are missing data files handled, or malformed SPO tuples included in the database input file. Testing parsing completeness would also be useful to avoid any weird off by one errors, though given the format this seems less likely.
3. Potential security issues related user input (providing PG user/pass, for example) if directly exposed to unsanitized access
4. Are dates all parsed correctly? 
5. Does searching for similar person names give logical results?

## Performance/Scaling

In lieu of detailed statistics, there are a few obvious performance issues: 

1. Querying the DB using what I anticipate to be common patterns (by name or by predicate) are slow given that these involve text comparisons; at least in the case of predicates, it may be wiser to recode the categories as something numeric to speed up comparisons. As it is, querying birthdates requires on the order of seconds, which is annoying even with a single user. 

2. Parsing the DBpedia input data can be done better, as type information is discarded during the parsing currently. Admittedly types can generally be inferred from the predicate but it would be better to make use of the type information somehow rather than storing everything as text (would also help with [1]). However, postgres is also not meant to store heterogenous types of data in a single column; you can wrap things in a JSON dict but that doesn't really solve the problem of how to store directly usable data in the DB.

3. Extracting basic statistics requires some additional parsing due to [1] if the column must be converted into an equivalent Python type (typically struct_time).

4. Inserting new SPO tuples will also be slow due to the need to update the indices on the tables. May be avoidable by batching updates. This might also be a symptom of the dataset size as the local machine has 16 GB of ram, and the database appears to be large enough to cause paging when accessed (sometimes).

5. A docker instance would be a better way to distribute this repository to avoid the need to modify any local postgres installation, as it stands the installation instructions are inappropriate for beginners and probably insecure.

If these issues are solved, I surmise scaling would involve using a distribute key-value store to fragment the database and its indices into something that can be held entirely in memory, then collating results (i.e. map/reduce).

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE) file for details
