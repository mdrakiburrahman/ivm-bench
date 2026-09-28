# Fabric post-batch validation

With `OPENIVM_VALIDATE=1`, `fabric-openivm-jvm-35` invokes validation after the
batch duration is recorded, alongside Spark's existing hook. It writes
`validation-fabric-openivm-jvm-35-batchN.json` and fails the batch on HTTP or
comparison failure. The non-IVM Fabric counterpart is unchanged.

Validation reuses the existing Spark validator's compiled-query recomputation,
column alignment, numeric normalization and row-multiset digest comparison.
This is a digest check, not an exact collision-free EXCEPT ALL proof. No new SQL
comparison semantics are introduced. Missing SQL/schema, failed models, empty
model sets and catalog selection failures cannot produce a successful Fabric
validation report.

The adapter uses the pinned dbt-fabricspark 1.9.5 `LivySession`/`LivyCursor`
implementation, attaching to dbt's existing persisted session in the resolved
compute lakehouse. It cannot intentionally create a replacement session or delete
the dbt-owned session. A fresh Fabric manifest is loaded from the build output,
not the compiler cache (lakehouse names are per-run).

Deterministic tests cover the transport adapter, validator wiring and failure
paths. **Cloud integration and SF100 parity have not been run or verified.**
After independent review, a real authorized GCI run must establish this evidence
before claiming Fabric validation passed. This change does not trigger a run or
alter engine pins. Performance versus previous commit and non-IVM counterpart:
not measured; validator work is outside the measured batch timer.
