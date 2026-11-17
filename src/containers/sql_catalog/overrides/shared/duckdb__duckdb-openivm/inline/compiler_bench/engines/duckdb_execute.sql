-- catalog-override: engine-only: DuckDB CLI marker queries bracket one input statement across multiple CLI statements.
SELECT '{begin}' AS __m;
{sql};
SELECT '{end}' AS __m;
