"""Standalone OpenIVM adapter implementation."""

import logging
from typing import List, Optional, Sequence

import agate
from dbt.adapters.base import BaseRelation, Column
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.openivm.connections import OpenIVMConnectionManager

logger = logging.getLogger(__name__)


class OpenIVMAdapter(SQLAdapter):
    """Adapter that routes all SQL through the OpenIVM CLI binary."""

    ConnectionManager = OpenIVMConnectionManager

    @classmethod
    def type(cls):
        return "openivm"

    @classmethod
    def date_function(cls):
        return "now()"

    @classmethod
    def is_cancelable(cls):
        return False

    def drop_relation(self, relation: BaseRelation) -> None:
        if relation.type == "view":
            self.execute_macro("openivm__drop_relation", kwargs={"relation": relation})
        else:
            super().drop_relation(relation)

    def list_schemas(self, database: str) -> List[str]:
        results = self.execute_macro("openivm__list_schemas", kwargs={"database": database})
        return [row[0] for row in results]

    def check_schema_exists(self, database: str, schema: str) -> bool:
        results = self.execute_macro(
            "openivm__check_schema_exists",
            kwargs={"information_schema": None, "schema": schema},
        )
        return results.rows != 0
