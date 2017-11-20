#!/bin/bash

function usage() {
    echo "
    Usage :
        ./data_diff.sh [OPTIONS] -- \"-h source\" \"-h target\" DATABASE [TABLE]
            -d DATA_PATH <= directory to the data storage (for csv export). DEFAULT : /data/dataDiff
            -r FILE <= summary report file. DEFAULT : ./report.log
            -e [tmiss],[smiss],[update] <= extract tmiss (missing on target), smiss (missing on source) or updated data to csv file named DATA_PATH/DATABASE.TABLE.csv
            -u <= use UNIQUE INDEX instead of PRIMARY KEY
            -t <= consider timestamp column values (ignore is DEFAULT)
            -c <= count only (-e disables this option) (DEFAULT)

    Example :
        ./data_diff.sh -d/data/dataDiff -r report.log -e tmiss,update -- \"-h dev\" \"-h prod\" mydatabase mytable
        => Will compare mytable in mydatabase and write mysqldump file for itarget missing and updated tuples
        => the csv file should be imported with the following SQL command :
            LOAD DATA LOCAL INFILE 'DATA_PATH/DATABASE_TABLE.csv' REPLACE INTO TABLE \`TABLE\` CHARACTER SET UTF8 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
"

}

DATE_DEBUT=$(date)

DATA_PATH="/data/dataDiff"
REPORT_LOG="./report.log"
EXPORT_TARGET_MISSING="0"
EXPORT_SOURCE_MISSING="0"
EXPORT_UPDATE="0"
EXCLUDE_TIMESTAMP="1"
COUNT_ONLY="false"
UNIQUE_INDEX="0"
SPECIAL_CHAR="¤"

while getopts "hd:r:e:utc" option
do
    case $option in
        h)
            usage
            exit
            ;;
        d)
            DATA_PATH=$OPTARG
            ;;
        r)
            REPORT_LOG=$OPTARG
            ;;
        e)
            EXPORT_TARGET_MISSING=$(grep -cw tmiss <<< "$OPTARG")
            EXPORT_SOURCE_MISSING=$(grep -cw smiss <<< "$OPTARG")
            EXPORT_UPDATE=$(grep -cw update <<< "$OPTARG")
            ;;
        u)
            UNIQUE_INDEX="1"
            ;;
        t)
            EXCLUDE_TIMESTAMP="0"
            ;;
        c)
            COUNT_ONLY="true"
            ;;
        \?)
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))
SOURCE=$1
TARGET=$2
DATABASE=$3
shift 3
TABLE_LIST=$@

if [ -z "$SOURCE" -o -z "$TARGET" -o -z "$DATABASE" ]
then
    usage
    exit
fi

if [ "$TABLE_LIST" = "" ]
then
    echo Guessing table list
    TABLE_LIST=$(mysql --skip-column-names -B $SOURCE $DATABASE <<< 'SHOW TABLES')
fi

echo Options activated :
echo ___ data path : $DATA_PATH
echo ___ report log : $REPORT_LOG
echo ___ export tmiss : $EXPORT_TARGET_MISSING
echo ___ export smiss : $EXPORT_SOURCE_MISSING
echo ___ export update : $EXPORT_UPDATE
echo ___ exclude timestamp : $EXCLUDE_TIMESTAMP
echo ___ use UNIQUE INDEX instead of PK : $UNIQUE_INDEX
echo ___ database : $DATABASE
echo ___ table list : $TABLE_LIST

[ -f $REPORT_LOG ] && rm $REPORT_LOG
mkdir -p $DATA_PATH

function data_count() {
    TABLE=$1
    pid_1=0
    pid_2=0

    [ -f $DATA_PATH/source_count_$TABLE.txt ] && rm $DATA_PATH/source_count_$TABLE.txt
    [ -f $DATA_PATH/target_count_$TABLE.txt ] && rm $DATA_PATH/target_count_$TABLE.txt

    echo "Launching count for table => $TABLE <="

    echo "SELECT COUNT(1) FROM $TABLE" | mysql -C --skip-column-names $SOURCE $DATABASE > $DATA_PATH/source_count_$TABLE.txt &
    pid_1=$!
    echo "SELECT COUNT(1) FROM $TABLE" | mysql -C --skip-column-names $TARGET $DATABASE > $DATA_PATH/target_count_$TABLE.txt &
    pid_2=$!

    sleep 2
    if [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
    then
        echo -n "Waiting for pids : $pid_1 or $pid_2 ..."
    fi
    while [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
    do
        echo -n "."
        sleep 5
    done
    echo "Count finished"

    if [ "$(cat $DATA_PATH/source_count_$TABLE.txt)" != "$(cat $DATA_PATH/target_count_$TABLE.txt)" ]
    then
        echo "Table => $TABLE <= ERRCOUNT : $(cat $DATA_PATH/source_count_$TABLE.txt) <> $(cat $DATA_PATH/target_count_$TABLE.txt)" >> $REPORT_LOG
        return 1
    else
        echo "Table => $TABLE <= COUNT OK : $(cat $DATA_PATH/source_count_$TABLE.txt)" >> $REPORT_LOG
        return 0
    fi
}

function data_diff() {
    TABLE=$1
    pid_1=0
    pid_2=0

    [ -f $DATA_PATH/source_diff_$TABLE.txt ] && rm $DATA_PATH/source_diff_$TABLE.txt
    [ -f $DATA_PATH/target_diff_$TABLE.txt ] && rm $DATA_PATH/target_diff_$TABLE.txt

    echo "Launching hash for table => $TABLE <="
    echo "CALL test.hashTable('$DATABASE', '$TABLE', '$SPECIAL_CHAR', $EXCLUDE_TIMESTAMP, $UNIQUE_INDEX)" | mysql -C --skip-column-names $SOURCE | grep -v "IGNORE THIS LINE" | sort > $DATA_PATH/source_diff_$TABLE.txt &
    pid_1=$!
    echo "CALL test.hashTable('$DATABASE', '$TABLE', '$SPECIAL_CHAR', $EXCLUDE_TIMESTAMP, $UNIQUE_INDEX)" | mysql -C --skip-column-names $TARGET | grep -v "IGNORE THIS LINE" | sort > $DATA_PATH/target_diff_$TABLE.txt &
    pid_2=$!

    sleep 2
    if [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
    then
        echo -n "Waiting for pids : $pid_1 or $pid_2 ..."
    fi
    while [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
    do
        echo -n "."
        sleep 5
    done
    echo "Hash finished"

    res=$(diff $DATA_PATH/source_diff_$TABLE.txt $DATA_PATH/target_diff_$TABLE.txt)
    if [ -n "$res" ]
    then
        echo "Table => $TABLE <= ERRDATADIFF" >> $REPORT_LOG
    else
        echo "Table => $TABLE <= DIFF OK" >> $REPORT_LOG
        return 0
    fi
}

function export_csv () {
    TABLE=$1

    [ -f $DATA_PATH/update_target_pk_list_${DATABASE}.${TABLE}.txt ] && rm $DATA_PATH/update_target_pk_list_${DATABASE}.${TABLE}.txt
    [ -f $DATA_PATH/update_source_pk_list_${DATABASE}.${TABLE}.txt ] && rm $DATA_PATH/update_source_pk_list_${DATABASE}.${TABLE}.txt
    [ -f $DATA_PATH/missing_target_pk_list_${DATABASE}.${TABLE}.txt ] && rm $DATA_PATH/missing_target_pk_list_${DATABASE}.${TABLE}.txt
    [ -f $DATA_PATH/missing_source_pk_list_${DATABASE}.${TABLE}.txt ] && rm $DATA_PATH/missing_source_pk_list_${DATABASE}.${TABLE}.txt

    touch $DATA_PATH/update_target_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/update_source_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/missing_target_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/missing_source_pk_list_${DATABASE}.${TABLE}.txt

    [ -f $DATA_PATH/to_target_${DATABASE}.${TABLE}.csv ] && rm $DATA_PATH/to_target_${DATABASE}.${TABLE}.csv
    [ -f $DATA_PATH/to_source_${DATABASE}.${TABLE}.csv ] && rm $DATA_PATH/to_source_${DATABASE}.${TABLE}.csv

    echo "Launching CSV export for table => $TABLE <="

    if [ "$EXPORT_TARGET_MISSING" != "0" -a -f $DATA_PATH/source_diff_$TABLE.txt -a -f $DATA_PATH/target_diff_$TABLE.txt ]
    then
        echo "Looking for target missing tuples..."
        comm -23 <(cut -f1 $DATA_PATH/source_diff_$TABLE.txt) <(cut -f1 $DATA_PATH/target_diff_$TABLE.txt) >> $DATA_PATH/missing_target_pk_list_${DATABASE}.${TABLE}.txt
    fi
    if [ "$EXPORT_SOURCE_MISSING" != "0" -a -f $DATA_PATH/source_diff_$TABLE.txt -a -f $DATA_PATH/target_diff_$TABLE.txt ]
    then
        echo "Looking for source missing tuples..."
        comm -13 <(cut -f1 $DATA_PATH/source_diff_$TABLE.txt) <(cut -f1 $DATA_PATH/target_diff_$TABLE.txt) >> $DATA_PATH/missing_source_pk_list_${DATABASE}.${TABLE}.txt
    fi
    if [ "$EXPORT_UPDATE" != "0" -a -f $DATA_PATH/source_diff_$TABLE.txt -a -f $DATA_PATH/target_diff_$TABLE.txt ]
    then
        echo "Looking for updated tuples..."
        grep -Fvf $DATA_PATH/source_diff_$TABLE.txt $DATA_PATH/target_diff_$TABLE.txt | grep -Ff <(cut -f1 $DATA_PATH/source_diff_$TABLE.txt) >> $DATA_PATH/update_target_pk_list_${DATABASE}.${TABLE}.txt
        grep -Fvf $DATA_PATH/target_diff_$TABLE.txt $DATA_PATH/source_diff_$TABLE.txt | grep -Ff <(cut -f1 $DATA_PATH/target_diff_$TABLE.txt) >> $DATA_PATH/update_source_pk_list_${DATABASE}.${TABLE}.txt
    fi

    if [ "$UNIQUE_INDEX" = "0" ]
    then
        pk_list=$(echo "
            SELECT GROUP_CONCAT(DISTINCT CONCAT('\`', COLUMN_NAME, '\`') ORDER BY COLUMN_NAME SEPARATOR ',')
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME = '$TABLE' AND COLUMN_KEY = 'PRI'
            LIMIT 1;
        " | mysql -C --skip-column-names $SOURCE information_schema | tr -d "\`")
    else
        pk_list=$(echo "
            SELECT GROUP_CONCAT(DISTINCT CONCAT('\`', COLUMN_NAME, '\`') ORDER BY COLUMN_NAME SEPARATOR ',')
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME = '$TABLE'
            AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY'
            GROUP BY INDEX_NAME
            LIMIT 1;
        " | mysql -C --skip-column-names $SOURCE information_schema | tr -d "\`")
    fi
    echo "The following columns will be considered as primary key : $pk_list"
    SED_LEFT="^"
    SED_RIGHT="OR ("
    i=1
    for pk in $(sed "s/,/ /g" <<< $pk_list)
    do
        if [ $i -eq 1 ]
        then
            SED_LEFT="${SED_LEFT}\\([^${SPECIAL_CHAR}]*\\)"
            SED_RIGHT="${SED_RIGHT}$pk='\1'"
        else
            SED_LEFT="${SED_LEFT}¤\\([^${SPECIAL_CHAR}]*\\)"
            SED_RIGHT="${SED_RIGHT} AND $pk='\\$i'"
        fi
        let i=$(( $i + 1 ))
    done
    SED_LEFT="${SED_LEFT}\$"
    SED_RIGHT="${SED_RIGHT})"

    #echo "${SED_LEFT}"
    #echo "${SED_RIGHT}"

    WHERE="FALSE $(cut -f1 <(cat $DATA_PATH/update_target_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/update_source_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/missing_target_pk_list_${DATABASE}.${TABLE}.txt $DATA_PATH/missing_source_pk_list_${DATABASE}.${TABLE}.txt ) |  sed "s/'/\\\'/g" | sed "s/${SED_LEFT}/${SED_RIGHT}/") "
    #echo $WHERE
    echo -n "CSV export..."
    echo "SELECT * FROM $TABLE WHERE $WHERE ORDER BY $pk_list" | mysql $SOURCE $DATABASE | cat > $DATA_PATH/to_target_${DATABASE}.${TABLE}.csv || return 1
    echo "SELECT * FROM $TABLE WHERE $WHERE ORDER BY $pk_list" | mysql $TARGET $DATABASE | cat > $DATA_PATH/to_source_${DATABASE}.${TABLE}.csv || return 1
    if [ "$EXPORT_SOURCE_MISSING" != "0" -a -f $DATA_PATH/source_diff_$TABLE.txt -a -f $DATA_PATH/target_diff_$TABLE.txt ]
    then
        echo "SELECT * FROM $TABLE WHERE $WHERE" | mysql $TARGET $DATABASE | cat > $DATA_PATH/to_source_${DATABASE}.${TABLE}.csv || return 1
    fi
    echo ".. ok!"
}

for table in $TABLE_LIST
do
    data_count $table
    if [ "$COUNT_ONLY" = "true" ]
    then
        continue
    fi

    data_diff $table
    if [ "$EXPORT_TARGET_MISSING" != "0" -o "$EXPORT_UPDATE" != "0" ]
    then
        export_csv $table
    fi
done

echo "Done"
echo "Time stats :"
echo "FROM $DATE_DEBUT TO $(date)"
