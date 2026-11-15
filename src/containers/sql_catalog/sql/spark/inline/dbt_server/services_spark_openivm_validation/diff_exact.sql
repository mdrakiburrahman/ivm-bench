WITH __ivm_mv_only AS (
  SELECT {projection} FROM {mv_relation}
  EXCEPT ALL
  SELECT {projection} FROM {expected_relation}
),
__ivm_expected_only AS (
  SELECT {projection} FROM {expected_relation}
  EXCEPT ALL
  SELECT {projection} FROM {mv_relation}
)
SELECT COUNT(*) FROM (
  SELECT * FROM __ivm_mv_only
  UNION ALL
  SELECT * FROM __ivm_expected_only
) AS __ivm_diff