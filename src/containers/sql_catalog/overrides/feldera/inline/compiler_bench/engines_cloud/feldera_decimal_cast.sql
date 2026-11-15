-- catalog-override: engine-only: Feldera normalizes decimal column types before calculating a comparison digest.
cast({column} AS {decimal_type})
