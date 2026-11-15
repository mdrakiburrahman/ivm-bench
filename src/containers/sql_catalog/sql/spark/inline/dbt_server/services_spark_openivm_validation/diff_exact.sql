SELECT COUNT(*) FROM (({mv_raw_subquery} EXCEPT ALL {expected_raw_subquery}) UNION ALL ({expected_raw_subquery} EXCEPT ALL {mv_raw_subquery})) AS __ivm_diff
