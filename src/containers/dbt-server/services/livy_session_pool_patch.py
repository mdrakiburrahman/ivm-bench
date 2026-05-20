from __future__ import annotations

import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.fabricspark.livysession import (
    LivyConnection,
    LivyCursor,
    LivySession,
    LivySessionManager,
    read_session_id_from_file,
    write_session_id_to_file,
)

logger = logging.getLogger(__name__)


@dataclass
class _LocalSessionPool:
    thread_sessions: Dict[int, LivySession] = field(default_factory=dict)
    available_session_ids: List[str] = field(default_factory=list)
    loaded_from_disk: bool = False


_POOL_LOCK = threading.Lock()
_POOL_BY_KEY: Dict[Tuple[str, str], _LocalSessionPool] = {}
_CREATE_MV_RE = re.compile(
    r"^\s*create\s+materialized\s+view\s+(?:if\s+not\s+exists\s+)?(?P<relation>[^\s(]+)",
    re.IGNORECASE | re.DOTALL,
)
_ORIGINAL_CONNECT = LivySessionManager.connect
_ORIGINAL_DISCONNECT = LivySessionManager.disconnect
_ORIGINAL_CURSOR_EXECUTE = LivyCursor.execute


def _pool_key(credentials) -> Tuple[str, str]:
    return (credentials.lakehouse_endpoint, credentials.resolved_session_id_file)


def _pool_manifest_path(session_id_file: str) -> str:
    return f"{session_id_file}.pool.json"


def _dedupe_session_ids(session_ids: List[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for session_id in session_ids:
        value = str(session_id).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _load_session_ids(session_id_file: str) -> List[str]:
    manifest_path = _pool_manifest_path(session_id_file)
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                session_ids = payload.get("session_ids", [])
            elif isinstance(payload, list):
                session_ids = payload
            else:
                session_ids = []
            return _dedupe_session_ids([str(session_id) for session_id in session_ids])
        except Exception as exc:
            logger.warning("Failed to read Livy session pool manifest %s: %s", manifest_path, exc)

    session_id = read_session_id_from_file(session_id_file)
    if session_id:
        return [session_id]
    return []


def _persist_session_ids(session_id_file: str, session_ids: List[str]) -> None:
    deduped = _dedupe_session_ids(session_ids)
    manifest_path = _pool_manifest_path(session_id_file)

    if not deduped:
        for path in (session_id_file, manifest_path):
            if os.path.exists(path):
                os.remove(path)
        return

    write_session_id_to_file(session_id_file, deduped[0])

    if len(deduped) == 1:
        if os.path.exists(manifest_path):
            os.remove(manifest_path)
        return

    manifest_dir = os.path.dirname(manifest_path)
    if manifest_dir and not os.path.exists(manifest_dir):
        os.makedirs(manifest_dir, exist_ok=True)

    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump({"session_ids": deduped}, handle)


def _get_pool_unlocked(credentials) -> _LocalSessionPool:
    key = _pool_key(credentials)
    pool = _POOL_BY_KEY.get(key)
    if pool is None:
        pool = _LocalSessionPool()
        _POOL_BY_KEY[key] = pool
    if not pool.loaded_from_disk:
        pool.available_session_ids = _load_session_ids(credentials.resolved_session_id_file)
        pool.loaded_from_disk = True
    return pool


def _set_current_session(session: LivySession) -> None:
    LivySessionManager.livy_global_session = session


def _assign_thread_session(credentials, thread_id: int, session: LivySession) -> None:
    with _POOL_LOCK:
        pool = _get_pool_unlocked(credentials)
        pool.thread_sessions[thread_id] = session
    _set_current_session(session)


def _patched_connect_local(credentials, spark_config) -> LivySession:
    thread_id = threading.get_ident()

    with _POOL_LOCK:
        pool = _get_pool_unlocked(credentials)
        session = pool.thread_sessions.get(thread_id)

    if session is not None and session.is_valid_session() and not session.is_new_session_required:
        _set_current_session(session)
        return session

    if session is not None:
        with _POOL_LOCK:
            pool = _get_pool_unlocked(credentials)
            current = pool.thread_sessions.get(thread_id)
            if current is session:
                pool.thread_sessions.pop(thread_id, None)

    while True:
        with _POOL_LOCK:
            pool = _get_pool_unlocked(credentials)
            existing_session_id = (
                pool.available_session_ids.pop(0) if pool.available_session_ids else None
            )

        if existing_session_id is None:
            break

        session = LivySession(credentials)
        if session.try_reuse_session(existing_session_id):
            _assign_thread_session(credentials, thread_id, session)
            return session

    session = LivySession(credentials)
    session.create_session(spark_config)
    session.is_new_session_required = False
    _assign_thread_session(credentials, thread_id, session)
    return session


def _patched_connect(credentials):
    if not credentials.is_local_mode:
        return _ORIGINAL_CONNECT(credentials)

    session = LivySessionManager._connect_local(credentials, credentials.spark_config)
    return LivyConnection(credentials, session)


def _normalize_relation_name(relation: str) -> str:
    return relation.replace("`", "").strip()


def _relation_exists(cursor: LivyCursor, relation: str) -> bool:
    relation_name = _normalize_relation_name(relation)
    probe = LivyCursor(cursor.credential, cursor.livy_session)
    _ORIGINAL_CURSOR_EXECUTE(probe, f"DESCRIBE TABLE {relation_name}")
    return bool(probe.fetchall() or probe.description)


def _can_treat_existing_mv_as_success(cursor: LivyCursor, sql: str, exc: Exception) -> bool:
    if not cursor.is_local_mode:
        return False
    if "table_or_view_already_exists" not in str(exc).lower():
        return False

    match = _CREATE_MV_RE.match(sql)
    if not match:
        return False

    try:
        return _relation_exists(cursor, match.group("relation"))
    except Exception:
        return False


def _patched_cursor_execute(self, sql: str, *parameters) -> None:
    try:
        return _ORIGINAL_CURSOR_EXECUTE(self, sql, *parameters)
    except DbtDatabaseError as exc:
        if _can_treat_existing_mv_as_success(self, sql, exc):
            self._rows = []
            self._schema = []
            return None
        raise


def _patched_disconnect() -> None:
    with _POOL_LOCK:
        local_pools = list(_POOL_BY_KEY.items())
        _POOL_BY_KEY.clear()
        local_session_ids = {
            session.session_id
            for _, pool in local_pools
            for session in pool.thread_sessions.values()
            if session.session_id is not None
        }
        global_session = LivySessionManager.livy_global_session
        if global_session is None or global_session.session_id in local_session_ids:
            LivySessionManager.livy_global_session = None
            global_session = None

    for (_, session_id_file), pool in local_pools:
        pooled_ids = list(pool.available_session_ids)
        for session in pool.thread_sessions.values():
            if session.session_id and session.is_valid_session() and not session.is_new_session_required:
                pooled_ids.append(str(session.session_id))
        _persist_session_ids(session_id_file, pooled_ids)

    if global_session is not None:
        LivySessionManager.livy_global_session = global_session
        return _ORIGINAL_DISCONNECT()

    if not local_pools:
        return _ORIGINAL_DISCONNECT()


def apply_patch() -> None:
    if getattr(LivySessionManager, "_openivm_thread_pool_patch", False):
        return

    LivySessionManager._connect_local = staticmethod(_patched_connect_local)
    LivySessionManager.connect = staticmethod(_patched_connect)
    LivySessionManager.disconnect = staticmethod(_patched_disconnect)
    LivyCursor.execute = _patched_cursor_execute
    LivySessionManager._openivm_thread_pool_patch = True


apply_patch()
