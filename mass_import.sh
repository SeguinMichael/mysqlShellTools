#!/bin/bash

#####
#
# Usage : ./mass_import.sh 80 "-h server -uroot -psecret" database1 database2 database3
#
#####

MAX_THREAD=$1
shift
CONNECTION_STRING=$1
shift
DATABASE_LIST=$@

DATE_DEBUT=$(date)
DATA_PATH="/data/sqlDump"

if [ -z "$MAX_THREAD" -o -z "$CONNECTION_STRING" -o -z "$DATABASE_LIST" ]
then
	echo "Usage : ./mass_import.sh 4 '-h server -uroot -psecret' database1 database2 database3"
	exit;
fi

MYSQL_CMD="mysql -C $CONNECTION_STRING"

mkdir -p ${DATA_PATH}/split

LOG_FILE=import.log
> $LOG_FILE

echo "[data] Splitting data file in background..."
for DATABASE in $DATABASE_LIST
do
	rm ${DATA_PATH}/split/${DATABASE}_data_*.sql 2>/dev/null
	head -n 17 ${DATA_PATH}/${DATABASE}_data.sql > ${DATA_PATH}/${DATABASE}_header.sql
	echo "SET AUTOCOMMIT=0;" >> ${DATA_PATH}/${DATABASE}_header.sql
	echo "BEGIN;" >> ${DATA_PATH}/${DATABASE}_header.sql
	echo "COMMIT;" > ${DATA_PATH}/${DATABASE}_footer.sql
	tail -n 11 ${DATA_PATH}/${DATABASE}_data.sql >> ${DATA_PATH}/${DATABASE}_footer.sql
	split -a 5 -d -n l/$(( $MAX_THREAD * 10 )) --additional-suffix=.sql ${DATA_PATH}/${DATABASE}_data.sql ${DATA_PATH}/split/${DATABASE}_data_
done &
PID_SPLIT=$!

echo "Warning: the specified databases will be restored in 10 seconds"
sleep 10

echo "[schema] Importing schemas..."
for DATABASE in $DATABASE_LIST
do
	echo "[schema] $DATABASE (2 times with -f in case of critical table dependencies) ..."
	$MYSQL_CMD -f $DATABASE < ${DATA_PATH}/${DATABASE}_struct.sql
	$MYSQL_CMD -f $DATABASE < ${DATA_PATH}/${DATABASE}_struct.sql
	echo "[schema] $DATABASE ok"
done

export MYSQL_CMD DATA_PATH

function go_mysql() {
	FILE=$1
	if [ ! -s $FILE ]
	then
		rm $FILE
	else
		DATABASE=$(basename $FILE | sed "s/_data.*sql//")
		lsof $FILE || ( cat ${DATA_PATH}/${DATABASE}_header.sql $FILE ${DATA_PATH}/${DATABASE}_footer.sql \
			| $MYSQL_CMD $DATABASE && rm $FILE )
	fi
}
export -f go_mysql

echo "[data] Importing 'ready to import' data"
while [ -n "$(ls ${DATA_PATH}/split/)" ]
do
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" ::: $(ls ${DATA_PATH}/split/* | sort -R)
	sleep 5
done

wait $PID_SPLIT

echo "[data] Importing remaining data"
while [ -n "$(ls ${DATA_PATH}/split/)" ]
do
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" ::: $(ls ${DATA_PATH}/split/* | sort -R)
done

rm ${DATA_PATH}/*_header.sql ${DATA_PATH}/*_footer.sql

echo "date debut=$DATE_DEBUT ; date fin=$(date)"
