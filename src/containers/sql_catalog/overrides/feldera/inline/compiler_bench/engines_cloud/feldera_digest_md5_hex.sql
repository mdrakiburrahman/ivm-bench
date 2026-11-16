-- catalog-override: engine-only: Feldera converts normalized row values to an eight-digit MD5 prefix for the bag digest.
substr(md5(concat_ws(chr(1), {parts})), 1, 8)
