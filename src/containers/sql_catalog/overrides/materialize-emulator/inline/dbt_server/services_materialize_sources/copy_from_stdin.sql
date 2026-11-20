-- catalog-override: Materialize ingests native tables using the PostgreSQL COPY protocol.
COPY {relation} ({columns}) FROM STDIN WITH (FORMAT TEXT)
