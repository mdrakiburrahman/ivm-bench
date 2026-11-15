-- catalog-override: engine-only: DBSPNet emulates dbt_utils null handling while compiling its translated program.
coalesce(cast({field} as varchar), '{null_value}')
