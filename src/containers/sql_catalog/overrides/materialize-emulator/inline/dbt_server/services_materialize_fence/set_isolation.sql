-- catalog-override: Materialize requires explicit strict-serializable isolation for the fence.
SET TRANSACTION ISOLATION LEVEL STRICT SERIALIZABLE
