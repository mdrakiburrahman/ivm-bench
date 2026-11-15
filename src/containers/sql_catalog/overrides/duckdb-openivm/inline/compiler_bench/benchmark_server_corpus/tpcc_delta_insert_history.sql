-- catalog-override: DuckDB uses quoted timestamp strings in these DML assignments and casts them to the target TIMESTAMP columns.
INSERT INTO HISTORY VALUES ({c}, {d}, {w}, {d}, {w}, '{date}', {amt}.00, 'Payment')
