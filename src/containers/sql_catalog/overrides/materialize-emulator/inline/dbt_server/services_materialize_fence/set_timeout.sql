-- catalog-override: Materialize bounds each strict read with a session statement timeout.
SET statement_timeout = '{seconds}s'
