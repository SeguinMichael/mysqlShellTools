#!/bin/bash

function usage() {
    echo "
    Usage :
        ./mass_dump.sh [OPTIONS] extra arguments
            -h <= this help
            -i <= incremental mode (append data to datafiles)
            -l z|t|n <= display type : z(enity) / t(erminal) / n(one)
            -d DATA_PATH <= directory to the data storage
            -n MAX_THREAD <= Multithreading mode
            -c <= don't check dump files
            -t <= no create info
            -B <= use extra arguments as database list instead of database_name and tables (mysqldump syntax)
            -s \"-h server1 -uroot\" <= connection string to send to mysqldump
"

}

DATE_DEBUT=$(date +"%Y-%m-%d %H:%M:%S")

INCREMENTAL="false"
DISPLAY_TO="z"
DATA_PATH="/data/sqlDump"
MAX_THREAD=1
DONTCHECK="false"
DATA_ONLY="false"
MULTIDATABASES="false"

while getopts "hil:d:n:ctBs:" option
do
    case $option in
        h)
            usage
            exit
            ;;
        i)
            INCREMENTAL="true"
            ;;
        l)
            DISPLAY_TO=$OPTARG
            ;;
        d)
            DATA_PATH=$OPTARG
            ;;
        n)
            MAX_THREAD=$OPTARG
            ;;
        c)
            DONTCHECK="true"
            ;;
        t)
            DATA_ONLY="true"
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

if [[ "$DISPLAY_TO" = "z" && ( -z "$DISPLAY" || -z "$(command -v zenity)" ) ]]
then
    DISPLAY_TO="t"
fi

if [ ! "$DISPLAY_TO" = "n" ]
then
    echo Options activated :
    echo ___ incremental : $INCREMENTAL
    echo ___ display type : $DISPLAY_TO
    echo ___ data path : $DATA_PATH
    echo ___ max threads : $MAX_THREAD
    echo ___ don\'t check files : $DONTCHECK
    echo ___ no create info : $DATA_ONLY
    echo ___ multi databases : $MULTIDATABASES
    echo ___ connection : $CONNECTION_STRING
fi

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
    if [ "$PARALLEL_TABLE_LIST" = "" ]
    then
        usage
        exit
    fi
fi

#checking mandatory options
if [ -z "$DATA_PATH" -o -z "$MAX_THREAD" -o -z "$MULTIDATABASES" -o -z "$CONNECTION_STRING" ]
then
    usage
    exit
fi

mkdir -p $DATA_PATH

MYSQLDUMP_STRUCT="mysqldump -d -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"
MYSQLDUMP_DATA="mysqldump --net_buffer_length=6144 --set-charset --quick --replace -t --skip-triggers -C --skip-disable-keys --skip-add-locks --skip-lock-tables --single-transaction $CONNECTION_STRING"

LOG_FILE=${DATA_PATH}/dump.log
if [ "$INCREMENTAL" = "false" ]
then
    > $LOG_FILE
fi

PARALLEL_OPTIONS="--silent --will-cite --retries 5 --jobs $MAX_THREAD --joblog $LOG_FILE "
if [ ! "$DISPLAY_TO" = "n" ]
then
    PARALLEL_OPTIONS="$PARALLEL_OPTIONS --eta --progress "
fi
if [ "$DISPLAY_TO" = "z" ]
then
    PARALLEL_OPTIONS="$PARALLEL_OPTIONS --bar "
fi

export MYSQLDUMP_STRUCT MYSQLDUMP_DATA DATA_PATH INCREMENTAL

function go_mysqldump() {
    TYPE=$1
    DATABASE=$(cut -d: -f1 <<< "$2")
    TABLE=$(cut -d: -f2 <<< "$2")

    CMD=""
    REDIRECTION=">"
    if [ "$TYPE" = "struct" ]
    then
        CMD=$MYSQLDUMP_STRUCT
    else
        if [ "$TYPE" = "data" ]
        then
            CMD=$MYSQLDUMP_DATA
            if [ "$INCREMENTAL" = "true" ]
            then
                REDIRECTION=">>"
            fi
        fi
    fi
    flock --no-fork --exclusive --wait 5 ${DATA_PATH}/${DATABASE}:${TABLE}_${TYPE}.sql.lz4 sh -c \
    "eval \"$CMD $DATABASE $TABLE | lz4 -9 $REDIRECTION ${DATA_PATH}/${DATABASE}:${TABLE}_${TYPE}.sql.lz4 || return 1\""
}
export -f go_mysqldump

if [ "$MULTIDATABASES" = "true" ]
then
    [ ! "$DISPLAY_TO" = "n" ] && echo "Guessing tables list"
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

if [ "$DATA_ONLY" = "false" ]
then
    [ ! "$DISPLAY_TO" = "n" ] && echo "Dumping schema"
    if [ "$DISPLAY_TO" = "z" ]
    then
        parallel $PARALLEL_OPTIONS "go_mysqldump struct {}" ::: $PARALLEL_TABLE_LIST 2> >(zenity --progress --auto-kill --auto-close --no-cancel)
    else
        parallel $PARALLEL_OPTIONS "go_mysqldump struct {}" ::: $PARALLEL_TABLE_LIST
    fi
fi

[ ! "$DISPLAY_TO" = "n" ] && echo "Dumping data"
if [ "$DISPLAY_TO" = "z" ]
then
    parallel $PARALLEL_OPTIONS "go_mysqldump data {}" ::: $PARALLEL_TABLE_LIST 2> >(zenity --progress --auto-kill --auto-close --no-cancel)
else
    parallel $PARALLEL_OPTIONS "go_mysqldump data {}" ::: $PARALLEL_TABLE_LIST
fi

if [ "$DONTCHECK" = "false" ]
then
    [ ! "$DISPLAY_TO" = "n" ] && echo "Checking integrity..."
    ls ${DATA_PATH}/*lz4 | while read LZ4_FILE
    do
        lz4 -ft $LZ4_FILE 2>/dev/null || echo "$LZ4_FILE is crashed :(" >&2
    done
    [ ! "$DISPLAY_TO" = "n" ] && echo "Done"
fi

[ ! "$DISPLAY_TO" = "n" ] && echo -e "Started at $DATE_DEBUT\nEnded at $(date +"%Y-%m-%d %H:%M:%S")"
exit 0
