import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.compiler_bench_corpus import (  # noqa: E402
    CorpusPaths,
    CorpusQuery,
    Translator,
    _render_type,
    prepare,
    read_corpus,
)


class ReadCorpusTest(unittest.TestCase):
    def test_ducklake_queries_are_optional_and_storage_agnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "query_0001.sql").write_text(
                "SELECT * FROM WAREHOUSE;\n", encoding="utf-8"
            )
            (root / "ducklake_0001.sql").write_text(
                "SELECT * FROM dl.WAREHOUSE JOIN DL.DISTRICT USING (W_ID);\n",
                encoding="utf-8",
            )

            generic = read_corpus(root)
            complete = read_corpus(root, include_ducklake=True)

        self.assertEqual([query.name for query in generic], ["query_0001"])
        self.assertEqual(
            [query.name for query in complete],
            ["ducklake_0001", "query_0001"],
        )
        self.assertEqual(
            complete[0].sql,
            "SELECT * FROM WAREHOUSE JOIN DISTRICT USING (W_ID)",
        )


class ResilientTranslationTest(unittest.TestCase):
    def test_feldera_schema_uses_supported_numeric_type_names(self):
        self.assertEqual(_render_type("FLOAT", "feldera"), "REAL")
        self.assertEqual(_render_type("INT", "feldera"), "INTEGER")

    def test_translation_wraps_query_to_make_duplicate_outputs_unique(self):
        translator = Translator("duckdb", "lpts", [])
        script = translator._script(
            [CorpusQuery(name="dup", sql="SELECT a, a FROM t", meta={})],
            "spark",
        )

        self.assertIn(
            "PRAGMA lpts('SELECT * FROM (SELECT a, a FROM t) AS __lpts_input');",
            script,
        )

    def test_one_crashing_query_does_not_erase_the_rest_of_its_chunk(self):
        queries = [
            CorpusQuery(name=f"q{i}", sql=f"SELECT {i}", meta={})
            for i in range(8)
        ]
        translator = Translator("duckdb", "lpts", [], chunk_size=8)

        def fake_run(chunk, dialect):
            names = [query.name for query in chunk]
            if "q3" not in names:
                return {query.name: (True, query.sql) for query in chunk}
            crash = names.index("q3")
            return {
                query.name: (
                    (True, query.sql)
                    if index < crash
                    else (False, "translation failed")
                )
                for index, query in enumerate(chunk)
            }

        translator._run_chunk = fake_run
        result = translator.translate(queries, "duckdb")

        self.assertEqual(result["q3"], (False, "translation failed"))
        for query in queries[:3] + queries[4:]:
            self.assertEqual(result[query.name], (True, query.sql))


class NativeDuckDBCorpusTest(unittest.TestCase):
    def test_duckdb_uses_source_sql_without_lpts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mount = root / "mount"
            paths = CorpusPaths.from_repo(root)
            paths.duckdb_bin.parent.mkdir(parents=True)
            paths.duckdb_bin.write_text("unused", encoding="utf-8")
            paths.corpus_src.mkdir(parents=True)
            (paths.corpus_src / "query_0001.sql").write_text(
                "SELECT * FROM WAREHOUSE;\n", encoding="utf-8"
            )

            result = prepare(repo_dir=root, engines=["duckdb"], force=True)

            emitted = (
                mount
                / "compiler-bench"
                / "corpus"
                / "duckdb"
                / "queries"
                / "query_0001.sql"
            ).read_text(encoding="utf-8")

        self.assertEqual(result["dialects"]["duckdb"]["translated"], 1)
        self.assertEqual(emitted, "SELECT * FROM WAREHOUSE\n")


if __name__ == "__main__":
    unittest.main()
