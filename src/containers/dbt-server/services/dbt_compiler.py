"""dbt compiler service — on-demand compile with caching."""

import json
import logging
import os
import subprocess
import threading
from typing import Any

logger = logging.getLogger(__name__)

PROJECTS_DIR = "/app/dbt-projects"

# Cache: {engine: manifest_dict}
_manifest_cache: dict[str, dict[str, Any]] = {}
_compile_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()


def _get_compile_lock(engine: str) -> threading.Lock:
    """Get or create a per-engine compile lock."""
    with _global_lock:
        if engine not in _compile_locks:
            _compile_locks[engine] = threading.Lock()
        return _compile_locks[engine]


def get_manifest(engine: str, force_compile: bool = False) -> dict[str, Any] | None:
    """
    Get the dbt manifest for an engine. Compiles on-demand if not cached.
    Returns the parsed manifest.json dict, or None on failure.
    """
    if not force_compile and engine in _manifest_cache:
        return _manifest_cache[engine]

    lock = _get_compile_lock(engine)
    with lock:
        # Double-check after acquiring lock
        if not force_compile and engine in _manifest_cache:
            return _manifest_cache[engine]

        project_dir = os.path.join(PROJECTS_DIR, engine)
        if not os.path.isdir(project_dir):
            logger.error("No dbt project directory for engine '%s'", engine)
            return None

        manifest_path = os.path.join(project_dir, "target", "manifest.json")

        # If manifest exists and we're not forcing, use it
        if not force_compile and os.path.exists(manifest_path):
            try:
                with open(manifest_path) as f:
                    manifest = json.load(f)
                _manifest_cache[engine] = manifest
                logger.info("Loaded existing manifest for engine '%s'", engine)
                return manifest
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to read existing manifest for '%s': %s", engine, e)

        # Run dbt compile — delete stale manifest first to avoid loading outdated state
        logger.info("Running dbt compile for engine '%s'...", engine)
        if os.path.exists(manifest_path):
            os.remove(manifest_path)
        cmd = [
            "dbt", "compile",
            "--profiles-dir", project_dir,
            "--project-dir", project_dir,
            "--target", engine,
        ]

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = project_dir

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, env=env, timeout=300,
            )
            if result.returncode not in (0, 2):
                logger.error(
                    "dbt compile failed for '%s' (rc=%d): %s",
                    engine, result.returncode, result.stderr[-2000:]
                )
                return None
            if result.returncode == 2:
                logger.warning(
                    "dbt compile for '%s' completed with warnings (rc=2)",
                    engine,
                )
        except subprocess.TimeoutExpired:
            logger.error("dbt compile timed out for engine '%s'", engine)
            return None

        # Load the compiled manifest
        if not os.path.exists(manifest_path):
            logger.error("manifest.json not found after compile for '%s'", engine)
            return None

        with open(manifest_path) as f:
            manifest = json.load(f)

        _manifest_cache[engine] = manifest
        logger.info("Compiled and cached manifest for engine '%s'", engine)
        return manifest


def get_compiled_models(engine: str) -> dict[str, dict[str, Any]]:
    """
    Get compiled SQL for all models in an engine.
    Returns {unique_id: {"name": ..., "compiled_sql": ..., "resource_type": ..., "schema": ...}}
    """
    manifest = get_manifest(engine)
    if not manifest:
        return {}

    models = {}
    for uid, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") not in ("model", "snapshot"):
            continue
        compiled_sql = node.get("compiled_code") or node.get("compiled_sql", "")
        if not compiled_sql:
            continue
        models[uid] = {
            "name": node.get("name", uid.split(".")[-1]),
            "unique_id": uid,
            "compiled_sql": compiled_sql,
            "resource_type": node.get("resource_type", ""),
            "schema": node.get("schema", ""),
        }

    return models


def invalidate_cache(engine: str | None = None):
    """Clear the manifest cache for an engine or all engines."""
    if engine:
        _manifest_cache.pop(engine, None)
    else:
        _manifest_cache.clear()
