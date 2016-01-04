#!/bin/bash

function usage() {
	echo "
	Usage :
		./mass_dump.sh [OPTIONS] extra arguments
			-d DATA_PATH <= directory to the data storage
			-n MAX_THREAD <= Multithreading mode
			-s \"-h server1 -uroot\" <= server1
			-B <= use extra arguments as database list instead of database_name and tables (mysqldump syntax)
"

}

DATE_DEBUT=$(date)

MAX_THREAD=1
DATA_PATH="/data/sqlDump"
MULTIDATABASES="false"

while getopts "hd:n:Bs:" option
do
	case $option in
		h)
			usage
			exit
			;;
		d)
			DATA_PATH=$OPTARG
			;;
		n)
			MAX_THREAD=$OPTARG
			;;
		B)
			MULTIDATABASES="true"
			;;
		s)
			CONNECTION_STRING=$OPTARG
			;;
		\?)
			exit 1
			;;
	esac
done
shift $((OPTIND-1))

echo Options activated :
echo ___ data path : $DATA_PATH
echo ___ max threads : $MAX_THREAD
echo ___ multi databases : $MULTIDATABASES

if [ "$MULTIDATABASES" = "true" ]
then
	DATABASE_LIST=$@
else
	DATABASE=$1
	shift
	TABLE_LIST=$@
fi

mkdir -p $DATA_PATH

if [ -z "$DATA_PATH" -o -z "$MAX_THREAD" -o -z "$MULTIDATABASES" -o -z "$CONNECTION_STRING" ]
then
	usage
	exit
fi

MYSQLDUMP_STRUCT="mysqldump -d -R --triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"
MYSQLDUMP_DATA="mysqldump --replace -t --skip-triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"

echo Dumping schema
if [ "$MULTIDATABASES" = "true" ]
then
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_STRUCT {1} | lz4 > ${DATA_PATH}/{1}_struct.sql.lz4" ::: $DATABASE_LIST
else
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_STRUCT $DATABASE {1} | lz4 > ${DATA_PATH}/${DATABASE}:{1}_struct.sql.lz4" ::: $TABLE_LIST
fi
echo Dumping data
if [ "$MULTIDATABASES" = "true" ]
then
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_DATA {1} | lz4 > ${DATA_PATH}/{1}_data.sql.lz4" ::: $DATABASE_LIST
else
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_DATA $DATABASE {1} | lz4 > ${DATA_PATH}/${DATABASE}:{1}_data.sql.lz4" ::: $TABLE_LIST
fi

echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
