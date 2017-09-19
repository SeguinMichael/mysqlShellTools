#!/bin/bash

function usage() {
    echo "
    Usage :
        ./deadlock.sh [ACTION] [OPTIONS]
            ACTIONs available :
                extract
                    -e SQLSTATE[_ERROR#] <= SQLSTATE to extract and, optionnaly his particular error. Can be used more than once.
                        Defaults are :
                            -e 40001
                            -e HY000_1205
                            -e HY000_2013
                            -e 70100
                            -e 08S01_1047
                    -d DATA_PATH <= directory to the data storage (default: /data/apache_logs)
                    => Prerequisite : the data storage must be filled with gzipped apache logs
                    => for example : rsync --progress -a --delete root@front1:/var/log/apache2/*error*gz /data/apache_logs/front1/
                insert
                compile

            OPTIONS available :
                -s CONNECTION_STRING => Mysql connection string (for insert and compile actions) including database name to use
"

}

ACTION=$1
shift
if [ -z "$ACTION" ]
then
    usage
    exit
fi

ERROR_TO_EXTRACT=()
while getopts "hd:e:s:" option
do
    case $option in
        h)
            usage
            exit
            ;;
        d)
            DATA_PATH=$OPTARG
            ;;
        s)
            CONNECTION_STRING=$OPTARG
            ;;
        e)
            ERROR_TO_EXTRACT+=("$OPTARG")
            ;;
        \?)
            exit 1
            ;;
    esac
done

if [ -z "$DATA_PATH" ]
then
    DATA_PATH=/data/apache_logs
fi

if [ 0 -eq ${#ERROR_TO_EXTRACT[@]} ]
then
    ERROR_TO_EXTRACT+=("40001")
    ERROR_TO_EXTRACT+=("HY000_1205")
    ERROR_TO_EXTRACT+=("HY000_2013")
    ERROR_TO_EXTRACT+=("70100")
    ERROR_TO_EXTRACT+=("08S01_1047")
fi

echo Action : $ACTION
echo Options activated :
echo ___ data path : $DATA_PATH
echo ___ connection : $CONNECTION_STRING
for key in ${!ERROR_TO_EXTRACT[*]}
do
    echo ___ error to extract : ${ERROR_TO_EXTRACT[$key]}
done

case $ACTION in
    extract)
        #PATTERN="SQLSTATE(\[40001\]|\[HY000\]: General error: (1205|2013) |\[70100\]|\[08S01\]: Communication link failure: 1047 )"
        PATTERN="SQLSTATE("
        for key in ${!ERROR_TO_EXTRACT[*]}
        do
            #echo ${ERROR_TO_EXTRACT[$key]}
            SQLSTATE=$(echo "${ERROR_TO_EXTRACT[$key]}_" | cut -f1 -d"_")
            ERROR=$(echo "${ERROR_TO_EXTRACT[$key]}_" | cut -f2 -d"_")
            #echo "$SQLSTATE => $ERROR"
            if [ -z "$ERROR" ]
            then
                PATTERN="${PATTERN}\[$SQLSTATE\]"
            else
                PATTERN="${PATTERN}\[$SQLSTATE\]:[^:]*: ${ERROR}"
            fi

            if [ $key -ne $((${#ERROR_TO_EXTRACT[@]}-1)) ]
            then
                PATTERN="${PATTERN}|"
            fi
        done
        PATTERN="${PATTERN})"
        echo ___ regex pattern : $PATTERN

        echo Extract in progress...
        zcat $(find ${DATA_PATH} -name "*gz") | cat - $(find ${DATA_PATH} -name "*log") | grep -E "$PATTERN" | while read LINE
        do
            STAT=$(sed 's/.*SQLSTATE\[\([^]]*\)\]:[^:]*: \([^ ]*\) .*/\1_\2/' <<< "$LINE") #Default apache log format
            DATE=$(sed 's/\[\([^]]*\)\].*/\1/' <<< "$LINE") #Default apache log format
            DATE=$(date -d"$DATE" +"%Y-%m-%d %H:%M:%S") #SQL Format
            echo "${DATE}|${STAT}"
        done > $DATA_PATH/deadlock.extract
        echo " ok!"
        ;;
    insert)
        echo Insert in progress...
        mysql $CONNECTION_STRING <<< "DROP TABLE IF EXISTS __temp;"
        mysql $CONNECTION_STRING <<< "CREATE TABLE __temp ( date_stat DATETIME, stat VARCHAR(32) );"
        cat $DATA_PATH/deadlock.extract | while read LINE
        do
            DATE=$(cut -d"|" -f1 <<< "$LINE")
            STAT=$(cut -d"|" -f2 <<< "$LINE")
            mysql $CONNECTION_STRING <<< "INSERT INTO __temp VALUES('$DATE', '$STAT');"
        done
        echo " ok!"
        ;;
    compile)
        WHERE="WHERE date_stat IS NOT NULL"
        mysql -t $CONNECTION_STRING <<< "SELECT YEARWEEK(date_stat), stat, COUNT(stat) FROM __temp WHERE date_stat IS NOT NULL GROUP BY YEARWEEK(date_stat), stat ORDER BY YEARWEEK(date_stat) DESC, stat;"
        mysql -t $CONNECTION_STRING <<< "SELECT
            HOUR(date_stat),
            COUNT(stat),
            ROUND(COUNT(stat) * 100 / (SELECT COUNT(*) FROM __temp $WHERE ) , 2) AS pct
        FROM __temp $WHERE GROUP BY HOUR(date_stat) ORDER BY HOUR(date_stat) DESC;"
esac
