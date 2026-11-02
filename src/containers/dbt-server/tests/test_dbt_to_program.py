import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import jinja2


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services import dbt_to_program  # noqa: E402
from services.dbt_to_program import SURROGATE_NULL, render  # noqa: E402

DBSPNET_PROJECT = DBT_SERVER / "dbt-projects" / "dbspnet"

BRANCHED = (
    "select * from {{ ref('a') }}\n"
    "{% if env_var('TPCDI_BATCH_2_DAYS', '0') | int > 0 %}\n"
    "where augmented\n"
    "{% else %}\n"
    "where plain\n"
    "{% endif %}\n"
)


class RenderTest(unittest.TestCase):
    def test_ref_and_source_become_bare_names(self):
        sql = "select * from {{ ref('staging_trade') }} join {{ source('raw', 'batch1_hr') }}"
        self.assertEqual(render(sql), "select * from staging_trade join batch1_hr")

    def test_surrogate_key_matches_dbt_utils_shape(self):
        sql = "select {{ dbt_utils.generate_surrogate_key(['t_id', 't_dts']) }} as k"
        null = f"'{SURROGATE_NULL}'"
        self.assertEqual(
            render(sql),
            "select md5(cast("
            f"coalesce(cast(t_id as varchar), {null}) || '-' || "
            f"coalesce(cast(t_dts as varchar), {null}) as varchar)) as k",
        )

    def test_env_var_branch_uses_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            out = render(BRANCHED)
        self.assertIn("where plain", out)
        self.assertNotIn("augmented", out)

    def test_env_var_branch_follows_the_environment(self):
        with patch.dict(os.environ, {"TPCDI_BATCH_2_DAYS": "3"}):
            out = render(BRANCHED)
        self.assertIn("where augmented", out)
        self.assertNotIn("plain", out)

    def test_unknown_construct_fails_instead_of_rendering_empty(self):
        with self.assertRaises(jinja2.UndefinedError):
            render("select {{ var('not_provided') }}")


class BuildProgramTest(unittest.TestCase):
    def test_dbspnet_project_renders_to_plain_sql(self):
        # Every statement the DbspNet engine receives must be Jinja-free; an unrendered
        # `{%` reaches the engine's SQL lexer and fails the deploy.
        for days in ("0", "2"):
            with self.subTest(TPCDI_BATCH_2_DAYS=days), patch.dict(os.environ, {"TPCDI_BATCH_2_DAYS": days}):
                spec = dbt_to_program.build_program(str(DBSPNET_PROJECT))
                for stmt in spec["program"]:
                    self.assertNotIn("{%", stmt)
                    self.assertNotIn("{{", stmt)


if __name__ == "__main__":
    unittest.main()
