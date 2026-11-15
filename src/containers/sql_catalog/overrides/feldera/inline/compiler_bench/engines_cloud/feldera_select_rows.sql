-- catalog-override: engine-only: Feldera reads matching input rows from its ad-hoc query endpoint before translating deltas to ingress records.
SELECT * FROM {table} WHERE {where}
