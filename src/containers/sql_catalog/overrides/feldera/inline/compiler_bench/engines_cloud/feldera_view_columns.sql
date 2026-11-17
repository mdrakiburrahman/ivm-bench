-- catalog-override: engine-only: Feldera obtains output column names through a bounded ad-hoc read.
SELECT * FROM {mv_name} LIMIT 1
