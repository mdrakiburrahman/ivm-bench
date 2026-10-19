"""Run the read-only history audit in the existing diagnostic workflow."""
import json
import os
from pathlib import Path
import runpy

if not os.environ.get('FABRIC_WORKSPACE_ID'):
    # The workflow's container connectivity check receives relay settings only.
    print(json.dumps({'history_audit': 'skipped', 'reason': 'workspace not passed to container'}))
else:
    try:
        module = runpy.run_path(str(Path(__file__).with_name('audit-fabric-sf100-runtime.py')))
        module['main']()
        path = module['OUT'] / 'history.json'
        print('AUDIT_HISTORY_BEGIN')
        print(path.read_text())
        print('AUDIT_HISTORY_END')
    except Exception as error:
        print(json.dumps({'history_audit': 'failed', 'error_type': type(error).__name__}))
        raise SystemExit(1)
