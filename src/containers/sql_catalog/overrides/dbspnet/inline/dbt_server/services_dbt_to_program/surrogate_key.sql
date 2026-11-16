-- catalog-override: engine-only: DBSPNet emulates dbt_utils MD5 surrogate keys while compiling its translated program.
md5(cast({fields} as varchar))
