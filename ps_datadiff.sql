USE test ;

DELIMITER ;
DROP PROCEDURE IF EXISTS hashTable;
DELIMITER !!
CREATE PROCEDURE `hashTable`(__database VARCHAR(64), __table VARCHAR(64), __special_char CHAR(1),  __exclude_timestamp TINYINT, __use_unique TINYINT)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
BEGIN
	DECLARE __pk_list TEXT;
	DECLARE __column_list TEXT;

	DECLARE __offset BIGINT UNSIGNED DEFAULT 0;
	DECLARE __limit BIGINT UNSIGNED DEFAULT 10000;

	SET GLOBAL innodb_stats_on_metadata=0;

    -- Retrieve pk_list
    IF (__use_unique = TRUE)
    THEN
        SELECT GROUP_CONCAT(DISTINCT CONCAT('\`', COLUMN_NAME, '\`') ORDER BY COLUMN_NAME SEPARATOR ',') INTO __pk_list
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = __database AND TABLE_NAME = __table
        AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY'
        GROUP BY INDEX_NAME
        LIMIT 1;
    ELSE
        SELECT GROUP_CONCAT(DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY COLUMN_NAME SEPARATOR ",") INTO __pk_list
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = __database AND TABLE_NAME = __table AND COLUMN_KEY = "PRI"
        LIMIT 1;
    END IF;

    -- Retrieve column_list
	IF (__exclude_timestamp = TRUE)
	THEN
		SELECT GROUP_CONCAT(DISTINCT CONCAT("IFNULL(`", COLUMN_NAME , "`, 'NULL' )" ) ORDER BY COLUMN_NAME SEPARATOR ",") INTO __column_list
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = __database AND TABLE_NAME = __table AND COLUMN_KEY <> "PRI" AND COLUMN_TYPE <> "timestamp";
	ELSE
		SELECT GROUP_CONCAT(DISTINCT CONCAT("IFNULL(`", COLUMN_NAME , "`, 'NULL' )" ) ORDER BY COLUMN_NAME SEPARATOR ",") INTO __column_list
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = __database AND TABLE_NAME = __table AND COLUMN_KEY <> "PRI";
	END IF;

	SET GLOBAL innodb_stats_on_metadata=1;
	SET @__count = 0;
	SET @sql_query = CONCAT("SELECT COUNT(1) INTO @__count FROM ", __database, ".", __table);
--	SELECT @sql_query;
	PREPARE stmt FROM @sql_query;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	WHILE __offset < @__count DO
		SET @sql_query = CONCAT("SELECT CONCAT_WS('", __special_char, "', ", __pk_list ,"), MD5(CONCAT(", IFNULL(__column_list,"NULL"), ")) AS `IGNORE THIS LINE` FROM ", __database, ".", __table, " ORDER BY ", __pk_list, " LIMIT ", __offset, ",", __limit);
		-- SELECT @sql_query;
		-- SELECT __pk_list, __column_list, __database, __table, __offset, __limit;
		PREPARE stmt FROM @sql_query;
		EXECUTE stmt;
		SET __offset = __offset + __limit;
	END WHILE;
END;
!!
DELIMITER ;
