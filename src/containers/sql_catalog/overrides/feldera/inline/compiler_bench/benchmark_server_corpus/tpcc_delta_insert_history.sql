-- catalog-override: Feldera accepts quoted timestamp strings in these DML assignments; keep its native literal form rather than Spark's typed TIMESTAMP literal.
INSERT INTO HISTORY VALUES ({c}, {d}, {w}, {d}, {w}, '{date}', {amt}.00, 'Payment')
