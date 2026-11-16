-- catalog-override: Native mutations need a durable per-experiment replay guard.
CREATE TABLE {relation} (batch_num integer, status text)
