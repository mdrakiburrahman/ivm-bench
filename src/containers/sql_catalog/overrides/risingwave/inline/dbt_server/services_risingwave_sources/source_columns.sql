-- catalog-override: RisingWave information_schema exposes inferred source types for timestamp conversion.
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table} ORDER BY ordinal_position
