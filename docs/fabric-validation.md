# Fabric post-batch validation

Validation is opt-in with `OPENIVM_VALIDATE=1` for both `spark-openivm` and
`fabric-openivm-jvm-35`. The default is disabled at every scale factor, including
SF10. Checks run after the batch duration is recorded and write
`validation-<engine>-batchN.json`. HTTP or comparison failures fail the batch.
The non-IVM Fabric counterpart is unchanged.

The benchmark hook reuses Spark's compiled-query recomputation, column alignment,
numeric normalization and row-multiset digest comparison. This digest check is
not a collision-free proof. An explicit `{"exact": true}` request to
`POST /validate/<engine>/<run_id>` selects bidirectional `EXCEPT ALL` on unrounded
user columns; dataset size never selects that mode automatically.

Fabric validation attaches to the dbt-owned session in the resolved compute
lakehouse using the pinned dbt-fabricspark adapter. Each statement owns its
cursor and the session survives validation. An unavailable or lost driver fails
validation rather than intentionally creating a replacement. The validator reads
the current build manifest, bypassing compiler caches and on-demand compilation
because lakehouse names change per run. Wrong route engines, failed runs, failed
models, missing SQL/schema and empty model sets cannot pass Fabric validation.

Deterministic tests cover transport, validator wiring, opt-in behavior and failure
paths. The consolidated Fabric validator has not been verified by a cloud run.
Performance comparisons against the previous commit and non-IVM counterpart are
unavailable for this validation change; validation is outside the batch timer.
