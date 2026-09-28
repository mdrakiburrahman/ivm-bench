"""Attach-only adapter for post-batch validation in the existing Fabric driver.

Use the same pinned dbt-fabricspark transport as dbt, never create another driver
against its OpenIVM state. SQL comparison semantics belong to the Spark validator.
"""

import json
import os
from pathlib import Path


class FabricValidationClient:
    def __enter__(self):
        from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
        from dbt.adapters.fabricspark.livysession import LivySession

        resolved = json.loads(Path(os.environ.get("FABRIC_RESOLVED_PATH", "/tmp/fabric-resolved.json")).read_text())
        self.lakehouse = resolved["lakehouse_name"]
        workspace = os.environ.get("FABRIC_WORKSPACE_ID", "")
        if not workspace or not self.lakehouse or not resolved.get("lakehouse_id"):
            raise RuntimeError("Fabric validation requires this run's resolved workspace/lakehouse")
        self.credentials = FabricSparkCredentials(
            livy_mode="fabric", authentication="CLI", workspaceid=workspace,
            lakehouseid=resolved["lakehouse_id"], lakehouse=self.lakehouse,
            schema=self.lakehouse,
            endpoint=os.environ.get("FABRIC_API_BASE", "https://api.fabric.microsoft.com").rstrip("/") + "/v1",
            reuse_session=True, session_id_file="/tmp/fabric-openivm-jvm-35-livy.session-id",
        )
        self.session_id = Path(self.credentials.session_id_file).read_text().strip()
        if not self.session_id:
            raise RuntimeError("Fabric validation requires the existing dbt session ID")
        self.session = LivySession(self.credentials)
        if not self.session.try_reuse_session(self.session_id):
            raise RuntimeError("Fabric dbt session unavailable; validation must not create a replacement")
        return self

    def execute(self, sql):
        from dbt.adapters.fabricspark.livysession import LivyCursor

        # The native cursor's recovery path can create a session. Do not enter it.
        if self.session.is_new_session_required or self.session.session_id != self.session_id:
            raise RuntimeError("Fabric validation lost the original dbt session")
        cursor = LivyCursor(self.credentials, self.session)
        try:
            cursor.execute(sql)
            return {"output": {"status": "ok", "data": {"application/json": {"data": cursor.fetchall() or []}}}}
        except Exception as exc:
            # The shared validator uses RuntimeError to retain per-model failures.
            raise RuntimeError(f"Fabric validation statement failed: {exc}") from exc
        finally:
            cursor.close()

    def __exit__(self, *exc):
        # dbt owns the long-lived session; never close/delete it here.
        return False
