-- catalog-override: SQLGlot's Databricks renderer uses TRY_CAST for Spark TIMESTAMP literals; CAST preserves the source statement's strict conversion behavior.
INSERT INTO HISTORY VALUES ({c}, {d}, {w}, {d}, {w}, CAST('{date}' AS TIMESTAMP), {amt}.00, 'Payment')
