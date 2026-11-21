SELECT
  iid.engagement_path AS engagement_path_norm,
  SUM(COALESCE(iid.allocated_units, 0)) AS allocated_units
FROM stg_engagement_capacity_history AS iid
WHERE
  iid.relationship_type = 'purchase'
GROUP BY
  iid.engagement_path
