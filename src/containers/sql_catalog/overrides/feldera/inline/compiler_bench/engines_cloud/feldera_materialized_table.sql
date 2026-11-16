-- catalog-override: engine-only: Feldera marks each generated input table materialized for pipeline-side verification.
{stmt} WITH ('materialized' = 'true');
