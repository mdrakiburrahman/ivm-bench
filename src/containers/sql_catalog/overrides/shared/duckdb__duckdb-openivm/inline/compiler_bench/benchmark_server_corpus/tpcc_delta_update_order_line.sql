-- catalog-override: DuckDB uses quoted timestamp strings in these DML assignments and casts them to the target TIMESTAMP columns.
UPDATE ORDER_LINE SET OL_DELIVERY_D = '{date}' WHERE OL_W_ID = {w} AND OL_D_ID = {d} AND OL_O_ID = 1 AND OL_NUMBER = 1
