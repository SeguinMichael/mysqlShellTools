#!/bin/bash

#####
#
# Usage :
#			./mass_import.sh 80 "-h server -uroot -psecret" database1 database2 database3
#			./mass_import.sh resume 80 "-h server -uroot -psecret" database1 database2 database3
#
#####

MAX_THREAD=$1
if [ "$MAX_THREAD" = "resume" ]
then
	RESUME="true"
	shift
	MAX_THREAD=$1
	echo "Resume option activated"
else
	RESUME="false"
fi
shift

CONNECTION_STRING=$1
shift
DATABASE_LIST=$@

DATE_DEBUT=$(date)
DATA_PATH="/data/sqlDump"

if [ -z "$MAX_THREAD" -o -z "$CONNECTION_STRING" -o -z "$DATABASE_LIST" ]
then
	echo "Usage : ./mass_import.sh 4 '-h server -uroot -psecret' database1 database2 database3"
	echo "Usage : ./mass_import.sh resume 4 '-h server -uroot -psecret' database1 database2 database3"
	exit;
fi

MYSQL_CMD="mysql -C $CONNECTION_STRING"
LS_CMD="ls ${DATA_PATH}/split/"'*'" | egrep '(${DATABASE_LIST// /|})_data_[0-9]"'*'".sql'"

mkdir -p ${DATA_PATH}/split

LOG_FILE=import.log
> $LOG_FILE

if [ "$RESUME" = "false" ]
then
	echo "[data] Splitting data file in background..."
	for DATABASE in $DATABASE_LIST
	do
		rm ${DATA_PATH}/split/${DATABASE}_data_*.sql 2>/dev/null
		lz4cat ${DATA_PATH}/${DATABASE}_data.sql.lz4 | head -n 17 > ${DATA_PATH}/${DATABASE}_header.sql
		echo "SET AUTOCOMMIT=0;" >> ${DATA_PATH}/${DATABASE}_header.sql
		echo "BEGIN;" >> ${DATA_PATH}/${DATABASE}_header.sql
		echo "COMMIT;" > ${DATA_PATH}/${DATABASE}_footer.sql

		#Perf => assuming struct footer is similar to data footer
		lz4cat ${DATA_PATH}/${DATABASE}_struct.sql.lz4 | tail -n 11 >> ${DATA_PATH}/${DATABASE}_footer.sql

		lz4cat ${DATA_PATH}/${DATABASE}_data.sql.lz4 | split -a 6 -d -l 5 -u --additional-suffix=.sql - ${DATA_PATH}/split/${DATABASE}_data_
	done &
	PID_SPLIT=$!
	echo split PID = $PID_SPLIT
	trap "{ ps $PID_SPLIT >/dev/null && ( kill $PID_SPLIT ; echo 'Split killed' ) || echo 'Split finished normally. Resume is safe' ; exit 255; }" SIGTERM SIGKILL SIGABRT EXIT
fi

echo "Warning: the specified databases will be restored in 10 seconds"
sleep 10

if [ "$RESUME" = "false" ]
then
	echo "[schema] Importing schemas..."
	for DATABASE in $DATABASE_LIST
	do
		echo "[schema] $DATABASE (2 times with -f in case of critical table dependencies) ..."
		lz4cat ${DATA_PATH}/${DATABASE}_struct.sql.lz4 | $MYSQL_CMD -f $DATABASE
		lz4cat ${DATA_PATH}/${DATABASE}_struct.sql.lz4 | $MYSQL_CMD -f $DATABASE
		echo "[schema] $DATABASE ok"
	done
fi

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
while [ -n "$(eval $LS_CMD)" ]
do
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" ::: $(eval $LS_CMD | sort -R)
	sleep 5
done

if [ "$RESUME" = "false" ]
then
	echo "Waiting for split..."
	wait $PID_SPLIT
fi

echo "[data] Importing remaining data"
while [ -n "$(eval $LS_CMD)" ]
do
	parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" ::: $(eval $LS_CMD | sort -R)
done

rm ${DATA_PATH}/*_header.sql ${DATA_PATH}/*_footer.sql

echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
