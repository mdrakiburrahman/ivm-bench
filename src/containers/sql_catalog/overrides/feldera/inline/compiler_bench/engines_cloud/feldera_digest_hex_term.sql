-- catalog-override: engine-only: Feldera maps one hexadecimal digit to its weighted integer value because DataFusion has no hash-to-number cast.
(strpos('{digits}', substr({hex_expr}, {index}, 1)) - 1) * {place}
