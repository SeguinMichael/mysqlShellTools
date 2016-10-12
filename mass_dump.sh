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

    PARALLEL_TABLE_LIST=""
    for table in ${TABLE_LIST}
    do
        PARALLEL_TABLE_LIST="$PARALLEL_TABLE_LIST ${DATABASE}:${table}"
    done
fi

mkdir -p $DATA_PATH

if [ -z "$DATA_PATH" -o -z "$MAX_THREAD" -o -z "$MULTIDATABASES" -o -z "$CONNECTION_STRING" ]
then
	usage
	exit
fi

MYSQLDUMP_STRUCT="mysqldump -d -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"
MYSQLDUMP_DATA="mysqldump --net_buffer_length=6144 --set-charset --quick --replace -t --skip-triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"

export MYSQLDUMP_STRUCT MYSQLDUMP_DATA DATA_PATH

function go_mysqldump() {
    TYPE=$1
    DATABASE=$(cut -d: -f1 <<< "$2")
    TABLE=$(cut -d: -f2 <<< "$2")

    CMD=""
    if [ "$TYPE" = "struct" ]
    then
        CMD=$MYSQLDUMP_STRUCT
    else
        if [ "$TYPE" = "data" ]
        then
            CMD=$MYSQLDUMP_DATA
        fi
    fi
    $CMD $DATABASE $TABLE | lz4 -9 > ${DATA_PATH}/${DATABASE}:${TABLE}_${TYPE}.sql.lz4 || return 1
}
export -f go_mysqldump

LOG_FILE=${DATA_PATH}/dump.log
> $LOG_FILE

if [ "$MULTIDATABASES" = "true" ]
then
    echo Guessing tables list
    PARALLEL_TABLE_LIST=""
    for DATABASE in $DATABASE_LIST
    do
        TABLE_LIST=$(mysql --skip-column-names -B $CONNECTION_STRING $DATABASE <<< 'SHOW TABLES')
        for table in ${TABLE_LIST}
        do
            PARALLEL_TABLE_LIST="$PARALLEL_TABLE_LIST ${DATABASE}:${table}"
        done
    done
fi

echo Dumping schema
parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysqldump struct {}" ::: $PARALLEL_TABLE_LIST

echo Dumping data
parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysqldump data {}" ::: $PARALLEL_TABLE_LIST

echo "Checking integrity..."
ls ${DATA_PATH}/*lz4 | while read LZ4_FILE
do
lz4 -ft $LZ4_FILE 2>/dev/null || echo "$LZ4_FILE is crashed :("
done
echo "Done"

echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
