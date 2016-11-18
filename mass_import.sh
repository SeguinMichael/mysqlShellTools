#!/bin/bash

function usage() {
    echo "
    Usage :
        ./mass_import.sh [OPTIONS] database1 database2 database3 [...]
            -d DATA_PATH <= directory to the data storage
            -n MAX_THREAD <= Multithreading mode
            -t <= no structure (import data only)
            -r <= resume mode (don't split data file, and read files from the data path)
            -s \"-h server1 -uroot\" <= server1
            -s \"-h server2 -uroot\" <= server2
            -s [...]
"

}

DATE_DEBUT=$(date)

MYSQL_CMD="mysql -C "
RESUME="false"
MAX_THREAD=1
DATA_PATH="/data/sqlDump"
STRUCTURE="true"

declare -a CONNECTION_STRING_LIST
EXPORT_CONNECTION_STRING_LIST=""

CONNECTION_STRING_LIST=()
while getopts "hd:n:rts:" option
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
        t)
            STRUCTURE="false"
            ;;
        r)
            RESUME="true"
            ;;
        s)
            CONNECTION_STRING_LIST+=("$OPTARG")
            ;;
        \?)
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

echo Options activated :
echo ___ data path : $DATA_PATH
echo ___ resume : $RESUME
echo ___ max threads : $MAX_THREAD
echo ___ structure : $STRUCTURE
for key in "${!CONNECTION_STRING_LIST[*]}"
do
    echo ___ connection : ${CONNECTION_STRING_LIST[$key]}
done
EXPORT_CONNECTION_STRING_LIST=$(declare -p CONNECTION_STRING_LIST 2>/dev/null)

DATABASE_LIST=$@

echo ___ databases : $DATABASE_LIST


if [ -z "$DATA_PATH" -o -z "$STRUCTURE" -o -z "$RESUME" -o -z "$MAX_THREAD" -o -z "$EXPORT_CONNECTION_STRING_LIST" -o -z "$DATABASE_LIST" ]
then
    usage
    exit
fi

LS_CMD="find ${DATA_PATH}/split/ -iregex '.*\/\($(sed 's/ /\\|/g' <<< "${DATABASE_LIST}")\)\(:.*\)*_data_[0-9]"'*'".sql'"

mkdir -p ${DATA_PATH}/split

LOG_FILE=${DATA_PATH}/import.log
> $LOG_FILE

if [ "$RESUME" = "false" ]
then
    echo "[data] Removing old files..."
    eval $LS_CMD | while read FILE
    do
        rm -- $FILE
    done
    echo "[data] Splitting data files in background..."
    ( for FILE in $(ls ${DATA_PATH}/*lz4 2>/dev/null | egrep "(${DATABASE_LIST// /|})(:.*)*_struct.sql.lz4")
    do
        STRUCT_FILE=$FILE
        DATA_FILE=${FILE/_struct.sql.lz4/_data.sql.lz4}
        HEADER_FILE=${FILE/_struct.sql.lz4/_header.sql}
        FOOTER_FILE=${FILE/_struct.sql.lz4/_footer.sql}
        ID=$(basename $STRUCT_FILE | sed "s/_struct.sql.lz4$//")

        #Building header file
        #source is struct file (17th first lines)
        #we just remove the UNIQUE_CHECKS=0 option because "replace into" is used
        lz4 -dc ${STRUCT_FILE} | head -n 17 | sed "s/UNIQUE_CHECKS=0/UNIQUE_CHECKS=1/" > ${HEADER_FILE}
        echo "SET AUTOCOMMIT=0;" >> ${HEADER_FILE}
        echo "BEGIN;" >> ${HEADER_FILE}
        echo "COMMIT;" > ${FOOTER_FILE}

        #Perf => assuming that struct footer is similar to the data footer
        lz4 -dc ${STRUCT_FILE} | tail -n 11 >> ${FOOTER_FILE}

        lz4 -dc ${DATA_FILE} | split -a 6 -d -l 5 -u --additional-suffix=.sql - ${DATA_PATH}/split/${ID}_data_
    done && echo 'Split finished normally. Resume is safe' ) &
    PID_SPLIT=$!
    echo split PID = $PID_SPLIT
    trap "{ ps $PID_SPLIT >/dev/null && ( kill $PID_SPLIT ; echo 'Split killed : resume is not safe' ) ; exit 255; }" SIGTERM SIGKILL SIGABRT EXIT
fi

echo "Warning: the specified databases will be restored in 10 seconds"
sleep 10

if [ "$RESUME" = "false" -a "$STRUCTURE" = "true" ]
then
    echo "[schema] Importing schemas..."
    for FILE in $(ls ${DATA_PATH}/*lz4 2>/dev/null | egrep "(${DATABASE_LIST// /|})(:.*)*_struct.sql.lz4")
    do
        STRUCT_FILE=$FILE
        ID=$(basename $STRUCT_FILE | sed "s/_struct.sql.lz4$//")
        DATABASE=$(cut -d: -f1 <<< "$ID")

        echo "[schema] $ID (2 times per server with -f in case of critical table dependencies) ..."
        for key in "${!CONNECTION_STRING_LIST[*]}"
        do
            lz4 -dc ${STRUCT_FILE} | $MYSQL_CMD ${CONNECTION_STRING_LIST[$key]} -f $DATABASE
            lz4 -dc ${STRUCT_FILE} | $MYSQL_CMD ${CONNECTION_STRING_LIST[$key]} -f $DATABASE
        done

        echo "[schema] $ID ok"
    done
fi

export MYSQL_CMD DATA_PATH EXPORT_CONNECTION_STRING_LIST

function go_mysql() {
    eval $EXPORT_CONNECTION_STRING_LIST
    FILE=$1

    DATA_FILE=${FILE}
    ID=$(basename $DATA_FILE | sed "s/_data.*sql$//")
    DATABASE=$(cut -d: -f1 <<< "$ID")

    if [ ! -s $FILE ]
    then
        echo "Empty file deleted : $FILE"
        rm -- $FILE
    else
        if lsof $FILE
        then
            echo "$FILE is currently locked... Will try later."
            return 1
        else
            removeFile=1
            for key in "${!CONNECTION_STRING_LIST[*]}"
            do
                #echo Sending to \'${CONNECTION_STRING_LIST[$key]}\' ...
                eval "cat ${DATA_PATH}/${ID}_header.sql $FILE ${DATA_PATH}/${ID}_footer.sql | $MYSQL_CMD ${CONNECTION_STRING_LIST[$key]} $DATABASE || removeFile=0"
            done
            if [ "$removeFile" = "1" ]
            then
                rm -- $FILE
                return 0
            else
                echo "An error has occured while importing $FILE ... Will try later."
                return 1
            fi
        fi
    fi
}
export -f go_mysql

echo "[data] Importing 'ready to import' data"
while [ -n "$(eval $LS_CMD)" ]
do
    eval "$LS_CMD | sort -R" > ${DATA_PATH}/split/todo
    parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" :::: ${DATA_PATH}/split/todo
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
    eval "$LS_CMD | sort -R" > ${DATA_PATH}/split/todo
    parallel --retries 5 --eta --progress --jobs $MAX_THREAD --joblog $LOG_FILE "go_mysql {1}" :::: ${DATA_PATH}/split/todo
done

rm ${DATA_PATH}/*_header.sql ${DATA_PATH}/*_footer.sql ${DATA_PATH}/split/todo

echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
