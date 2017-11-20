#!/bin/bash


#####
# Usage : ./mysql.datadiff.sh "-h seth" "-h cluster2" reparmax_reference traduction 2>error.log 3>&2 4>ok.log
#
#
#####

SERVER_1=$1
SERVER_2=$2
DATABASE=$3
TABLE=$4

if [ -z "$SERVER_1" -o -z "$SERVER_2" ]
then
	echo "
	mysql.datadiff.sh
		IN :
		ARG 1 (mandatory) => source server connection string
		ARG 2 (mandatory) => target server connection string
		ARG 3 (optionnal) => database. if empty, all user databases
		ARG 4 (optionnal) => table. if empty, all tables

		OUT :
		stdout	=> what's going on
		stderr	=> sys error
		3		=> differences
		4		=> what's ok

		EXAMPLE :
		mysql.datadiff.sh server1 server2 database table 2>error.log 3>&2 4>ok.log
"
	exit

fi

FORCE_DIFF=1
DIFF_IF_COUNT_OK=1

MYSQL_1="mysql -C --skip-column-names $SERVER_1 test"
MYSQL_2="mysql -C --skip-column-names $SERVER_2 test"

function make_diff() {
	table=$1
	IFS="."
	set $table
	IFS=" "
	pid_1=0
	pid_2=0

	echo "DBG => launching hash" >&1

	echo "call test.hashTable('$1', '$2', 1)" | $MYSQL_1 | grep -v "IGNORE THIS LINE"> srv1.txt &
	pid_1=$!
	echo "call test.hashTable('$1', '$2', 1)" | $MYSQL_2 | grep -v "IGNORE THIS LINE"> srv2.txt &
	pid_2=$!

	sleep 2
	if [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
	then
		echo -n "DBG => waiting for pids : $pid_1 or $pid_2 ..." >&1
	fi
	while [ -n "$(ps -h $pid_1)" -o -n "$(ps -h $pid_2)" ]
	do
		echo -n "." >&1
		sleep 5
	done
	echo "DBG => hash finished" >&1

	echo "DBG => launching diff" >&1
	res=$(diff srv1.txt srv2.txt)
	echo "DBG => diff finished" >&1
	if [ -n "$res" ]
	then
		echo "Table <= $table => DATADIFF :" >&3
		echo $res >&3
		echo "Table <= $table => END DATADIFF" >&3
	else
		echo "Table <= $table => DIFF OK" >&4
	fi
}

if [ -z "$DATABASE" ]
then
	#No database means all databases
	TABLE_LIST=$( echo "SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS __dummy FROM information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'test');" | $MYSQL_1 )
else
	#Database specified ...
	if [ -n "$TABLE" ]
	then
		#Table specified
		TABLE_LIST=$( echo "SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS __dummy FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME= '$TABLE';" | $MYSQL_1 )
	else
		#No table means all tables
		TABLE_LIST=$( echo "SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS __dummy FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$DATABASE';" | $MYSQL_1 )
	fi
fi

for table in $TABLE_LIST
do
	if [[ $table != *__* ]] #ignore tables having "__" in name
	then
		echo "DBG => start table : $table" >&1

		count_1=$(echo "SELECT COUNT(1) FROM $table" | $MYSQL_1)
		count_2=$(echo "SELECT COUNT(1) FROM $table" | $MYSQL_2)

		if [ "$count_1" != "$count_2" ]
		then
			echo "Table <= $table => ERRCOUNT : $count_1 <> $count_2" >&3
			[ -n "$FORCE_DIFF" -a "$FORCE_DIFF" != "0" ] && make_diff $table
		else
			echo "Table <= $table => COUNT OK : $count_1" >&4
			[ -n "$DIFF_IF_COUNT_OK" -a "$DIFF_IF_COUNT_OK" != "0" ] && make_diff $table
		fi
		echo "DBG => end table : $table" >&1

		#trigger error
		count_1=0
		count_2=1
	fi
done
