"""Standalone dbt adapter for DuckDB-OpenIVM.

ALL SQL goes through the OpenIVM CLI binary subprocess.
"""
import os

from dbt.adapters.openivm.connections import OpenIVMConnectionManager
from dbt.adapters.openivm.impl import OpenIVMAdapter
from dbt.adapters.openivm.credentials import OpenIVMCredentials

PACKAGE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "include", "openivm")

from dbt.adapters.base import AdapterPlugin

Plugin = AdapterPlugin(
    adapter=OpenIVMAdapter,
    credentials=OpenIVMCredentials,
    include_path=PACKAGE_PATH,
)
