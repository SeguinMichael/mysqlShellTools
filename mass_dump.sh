#!/bin/bash

#####
#
# Usage : ./mass_dump.sh "-h server -uroot -psecret" database1 database2 database3
#
#####

CONNECTION_STRING=$1
shift
DATABASE_LIST=$@

DATE_DEBUT=$(date)
MAX_THREAD=4
DATA_PATH="/data/sqlDump"

mkdir -p $DATA_PATH

if [ -z "$CONNECTION_STRING" -o -z "$DATABASE_LIST" ]
then
	echo "Usage : ./mass_dump.sh '-h server -uroot -psecret' database1 database2 database3"
	exit;
fi

MYSQLDUMP_STRUCT="mysqldump -d -R --triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"
MYSQLDUMP_DATA="mysqldump --replace -t --skip-triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"

echo Dumping schema
parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_STRUCT {1} | lz4 > ${DATA_PATH}/{1}_struct.sql.lz4" ::: $DATABASE_LIST
echo Dumping data
parallel --retries 5 --eta --progress --jobs $MAX_THREAD "$MYSQLDUMP_DATA {1} | lz4 > ${DATA_PATH}/{1}_data.sql.lz4" ::: $DATABASE_LIST

echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
