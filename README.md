# mysqlShellTools
Some usefull scripts for mysql

## Usage
### mass\_dump.sh
Usage : ./mass\_dump.sh \[OPTIONS\] \[extra args\]
* OPTIONS :
** -d PATH <= directory path to the local storage (default: /data/sqlDump)
** -n THREADS <= maximum threads to use
** -s CONNSTR <= mysqldump connection string
** -B <= use extra args as databases instead of tables (mysqldump syntax)
* extra args <= database name followed by table list to dump (except if -B is used, see below)

### mass\_import.sh
Usage : ./mass\_import.sh \[OPTIONS\] \[extra args\]
* OPTIONS :
** -d PATH <= directory path to the local storage (default: /data/sqlDump)
** -n THREADS <= maximum threads to use
** -r <= resume mode (don't split files and don't restore schema)
** -s CONNSTR <= mysqldump connection string (can be used more than once)
* extra args <= database list to restore

## How it works
### mass\_dump.sh
* Create a thread per table (or database)
* Each thread launches...
** ... a mysqldump for the schema, located in ${DATA\_PATH}/${DATABASENAME}\[:${TABLENAME}\]\_struct.sql
** ... a mysqldump for the data, located in ${DATA\_PATH}/${DATABASENAME}\[:${TABLENAME}\]\_data.sql

### mass\_import.sh
* For each database, a header and a footer is extracted from the data files, theses 2 files are used to customize the mysql session
* The "split" command is used in background to split the data files into small pieces
* A warning is displayed
* The schema is imported twice with -f. Sometimes, there's dependencies between tables so it's safer to do this
* The parallel command is used as many time as needed to send each piece to the mysql command for the specified databases and every connection string

External tools needed :
[parallel](https://www.gnu.org/software/parallel/)
[mysql](https://www.mysql.com/)
[split](https://www.gnu.org/software/coreutils/manual/html_node/split-invocation.html)
