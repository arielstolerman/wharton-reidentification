INSERT INTO `<WEIGHTS_VECTOR_TABLE_NAME>` (`transactor_id`,`transactee_id`,`weight`)
	SELECT `transactor_id`,`transactee_id`,SUM(`weight`) FROM `transactions`
	WHERE `transactor_id` IN (SELECT `transactor_id` FROM `transactors` WHERE `transactor_name` REGEXP '<TRANSACTOR_NAME_FILTER>')
	AND `transactee_id` IN (SELECT `transactor_id` FROM `transactors` WHERE `transactor_name` REGEXP '<TRANSACTEE_NAME_FILTER>')
	GROUP BY `transactor_id`,`transactee_id`
	ORDER BY `transactor_id`,`transactee_id`;