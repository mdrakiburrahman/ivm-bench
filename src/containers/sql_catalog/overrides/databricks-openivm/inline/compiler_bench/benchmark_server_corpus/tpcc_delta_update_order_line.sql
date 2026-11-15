-- catalog-override: SQLGlot's Databricks renderer uses TRY_CAST for Spark TIMESTAMP literals; CAST preserves the source statement's strict conversion behavior.
UPDATE ORDER_LINE SET OL_DELIVERY_D = CAST('{date}' AS TIMESTAMP) WHERE OL_W_ID = {w} AND OL_D_ID = {d} AND OL_O_ID = 1 AND OL_NUMBER = 1
