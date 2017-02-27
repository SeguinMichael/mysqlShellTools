#!/bin/bash

function usage() {
    echo "
    Usage :
        ./deadlock.sh [ACTION] [OPTIONS]
            ACTIONs available :
                extract
                    -E REGEX => regex pattern to extract. Default is : \"SQLSTATE(\[40001\]|\[HY000\]: General error: (1205|2013) |\[70100\]|\[08S01\]: Communication link failure: 1047 )\"
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

while getopts "hd:E:s:" option
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
        E)
            PATTERN=$OPTARG
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
if [ -z "$PATTERN" ]
then
    PATTERN="SQLSTATE(\[40001\]|\[HY000\]: General error: (1205|2013) |\[70100\]|\[08S01\]: Communication link failure: 1047 )"
fi

echo Action : $ACTION
echo Options activated :
echo ___ data path : $DATA_PATH
echo ___ connection : $CONNECTION_STRING
echo ___ regex pattern : $PATTERN

case $ACTION in
    extract)
        echo Extract in progress...
        zcat $(find ${DATA_PATH} -name "*gz") | grep -E "$PATTERN" | while read LINE
        do
            DATE=$(sed 's/\[\([^]]*\)\].*/\1/' <<< "$LINE") #Default apache log format
            date -d"$DATE" +"%Y-%m-%d %H:%M:%S" #SQL Format
        done > $DATA_PATH/deadlock.extract
        echo " ok!"
        ;;
    insert)
        echo Insert in progress...
        mysql $CONNECTION_STRING <<< "DROP TABLE IF EXISTS __temp;"
        mysql $CONNECTION_STRING <<< "CREATE TABLE __temp ( date_stat DATE, stat TINYINT );"
        cat $DATA_PATH/deadlock.extract | while read DATE
        do
            mysql $CONNECTION_STRING <<< "INSERT INTO __temp VALUES('$DATE', 1);"
        done
        echo " ok!"
        ;;
    compile)
        mysql -t $CONNECTION_STRING <<< "SELECT YEARWEEK(date_stat), SUM(stat) FROM __temp WHERE date_stat IS NOT NULL GROUP BY YEARWEEK(date_stat) ORDER BY YEARWEEK(date_stat) DESC;"
esac
