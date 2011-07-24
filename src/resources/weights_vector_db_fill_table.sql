INSERT INTO `<WEIGHTS_VECTOR_TABLE_NAME>` (`transactor_id`,`transactee_id`,`weight`)
	SELECT `transactor_id`,`transactee_id`,SUM(`weight`) FROM `transactions`
	GROUP BY `transactor_id`,`transactee_id`
	ORDER BY `transactor_id`,`transactee_id`;