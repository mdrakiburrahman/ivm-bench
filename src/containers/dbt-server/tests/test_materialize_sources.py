import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from services import materialize_sources


class MaterializeCopyTests(unittest.TestCase):
    def test_text_copy_preserves_nulls_and_escaped_values(self):
        self.assertEqual(materialize_sources._copy_field(None), r"\N")
        self.assertEqual(materialize_sources._copy_field(r"\N"), r"\\N")
        self.assertEqual(materialize_sources._copy_field("a\tb\nc\r"), r"a\tb\nc\r")
        self.assertEqual(materialize_sources._copy_field(Decimal("12.30")), "12.30")
        self.assertEqual(materialize_sources._copy_field(date(2020, 2, 29)), "2020-02-29")

    def test_nested_decimal_is_not_silently_changed(self):
        with self.assertRaises(TypeError):
            materialize_sources._copy_field({"value": Decimal("12.30")})

    def test_chunk_failure_does_not_retry(self):
        batch = pa.RecordBatch.from_pylist([{"id": 1}, {"id": 1}, {"id": 2}])
        dataset = MagicMock()
        dataset.schema.names = ["id"]
        dataset.scanner.return_value.to_batches.return_value = [batch]
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.copy_expert.side_effect = [None, RuntimeError("write status unknown")]
        identifier = MagicMock()
        identifier.as_string.return_value = '"name"'
        joiner = MagicMock()
        joiner.join.return_value = identifier
        with (
            patch.object(materialize_sources, "DeltaTable") as delta,
            patch.object(materialize_sources.sql, "Identifier", return_value=identifier),
            patch.object(materialize_sources.sql, "SQL", return_value=joiner),
            patch.object(materialize_sources, "load_sql", return_value="copy"),
            patch.object(materialize_sources, "COPY_CHUNK_BYTES", 3),
        ):
            delta.return_value.to_pyarrow_dataset.return_value = dataset
            with self.assertRaisesRegex(RuntimeError, "write status unknown"):
                materialize_sources.copy_delta_snapshot(connection, Path("input"), "s", "t")
        self.assertEqual(cursor.copy_expert.call_count, 2)


if __name__ == "__main__":
    unittest.main()
