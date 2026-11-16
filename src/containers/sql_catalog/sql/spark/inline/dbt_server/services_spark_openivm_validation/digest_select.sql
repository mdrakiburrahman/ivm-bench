SELECT COUNT(*) AS __ivm_cnt, COALESCE(SUM(xxhash64({hash_args})), 0L) AS __ivm_hash FROM (SELECT {inner_projection} FROM {relation}) AS {alias}
