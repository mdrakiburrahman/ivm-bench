-- catalog-override: engine-only: DuckDB OpenIVM validation executes its extension-specific temporary-table and EXCEPT ALL script.
CREATE OR REPLACE TEMP TABLE {expected_name} AS
{compiled_sql};
SELECT {unique_id_sql} AS unique_id, COUNT(*) AS diff_count FROM (
    (SELECT * FROM {relation} EXCEPT ALL SELECT * FROM {expected_name})
    UNION ALL
    (SELECT * FROM {expected_name} EXCEPT ALL SELECT * FROM {relation})
) AS openivm_diff;
DROP TABLE IF EXISTS {expected_name};
