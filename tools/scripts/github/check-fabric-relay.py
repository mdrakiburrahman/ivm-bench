#!/usr/bin/env python3
"""Compare runner-to-relay reachability with the Fabric sidecar's auth logs.

Emit only status and timing; never print credentials, URLs or token contents.
This is diagnostic and leaves the engine's authentication gate in control.
"""

import importlib.util
import json
import os
import time
from pathlib import Path


root = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "relay_probe", root / "src/containers/imds-router/imds_relay_router.py"
)
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)
result = {"probe": "fabric-relay-from-runner"}
started = time.monotonic()
if not relay.RELAY_URL or not relay.RELAY_SENDER_KEY:
    result["status"] = "not_configured"
else:
    try:
        body, error = relay._relay_fetch("https://management.core.windows.net/", os.environ.get("UAMI_CLIENT_ID", "00000000-0000-0000-0000-000000000000"))
        result["status"] = "passed" if body and body.get("access_token") and not error else "failed"
    except Exception as error:
        result.update(status="failed", error_type=type(error).__name__)
        if getattr(error, "code", None) is not None:
            result["http_status"] = error.code
        if getattr(error, "reason", None) is not None:
            result["reason_type"] = type(error.reason).__name__
result["elapsed_s"] = round(time.monotonic() - started, 3)
print(json.dumps(result), flush=True)
