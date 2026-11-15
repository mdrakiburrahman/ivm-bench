-- catalog-override: RisingWave information_schema lookup is an engine-native pgwire metadata query.
SELECT column_name FROM information_schema.columns WHERE table_schema = {schema} AND table_name = {table} ORDER BY ordinal_position
