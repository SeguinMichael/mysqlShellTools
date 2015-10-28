# mysqlShellTools
Some usefull scripts for mysql

## Usage
### mass\_dump.sh
Usage : ./mass\_dump.sh '-h server -uroot -psecret' database1 database2 database3
* connection string
* database list to dump

### mass\_import.sh
Usage : ./mass\_import.sh 4 '-h server -uroot -psecret' database1 database2 database3
* maximum threads
* connection\_string
* database list to import

## How it works
### mass\_dump.sh
* Create a thread per database
* Each thread launches...
** ... a mysqldump for the schema, located in ${DATA\_PATH}/${DATABASENAME}\_struct.sql
** ... a mysqldump for the data, located in ${DATA\_PATH}/${DATABASENAME}\_data.sql

### mass\_import.sh
* For each database, a header and a footer is extracted from the data file, theses 2 files are used to customize the mysql session
* The command "split" is used in background to split the data file into pieces, each piece having now the header and the footer.
* A warning is displayed
* The schema is imported twice with -f. Sometimes, there's dependencies between tables so it's safer to do this.
* The parallel command is used as many time as needed to send each piece to the mysql command to the specified databases

External tools needed :
[parallel](https://www.gnu.org/software/parallel/)
[mysql](https://www.mysql.com/)
[split](https://www.gnu.org/software/coreutils/manual/html_node/split-invocation.html)
