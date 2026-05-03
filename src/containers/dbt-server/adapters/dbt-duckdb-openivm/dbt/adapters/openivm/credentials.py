"""OpenIVM credentials — minimal config for the CLI binary adapter."""

from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import Credentials


@dataclass
class OpenIVMCredentials(Credentials):
    """Connection credentials for the OpenIVM CLI adapter."""
    database: str = "ducklake"
    schema: str = "main"
    bin_path: Optional[str] = None
    work_dir: Optional[str] = None

    @property
    def type(self):
        return "openivm"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return ("database", "schema", "bin_path", "work_dir")
