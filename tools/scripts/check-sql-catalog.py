import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTAINERS_DIR = ROOT / "src" / "containers"
EXCEPTIONS_DIR = CONTAINERS_DIR / "sql_catalog" / "exceptions"
sys.path.insert(0, str(CONTAINERS_DIR))

from sql_catalog.generator import _line_marker, is_sql_candidate, render_sql


def _git(*args):
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _exceptions():
    exceptions = []
    for path in sorted(EXCEPTIONS_DIR.glob("*.json")):
        entries = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(entries, list):
            raise ValueError(f"{path}: exceptions file must contain a JSON list")
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("path"), str)
                or not entry["path"]
                or not isinstance(entry.get("reason"), str)
                or not entry["reason"].strip()
            ):
                raise ValueError(f"{path}: each exception requires a path and reason")
            exceptions.append(entry)
    return exceptions


def _is_exception(path, exceptions):
    normalized = path.replace("\\", "/")
    if "/tests/" in f"/{normalized}/":
        return True
    return any(fnmatch.fnmatch(path, item["path"]) for item in exceptions)


def _changed_paths(base_ref):
    tracked = set(_git("diff", "--name-only", base_ref, "--").splitlines())
    untracked = _git("ls-files", "--others", "--exclude-standard").splitlines()
    tracked.update(untracked)
    return sorted(path for path in tracked if path)


def _added_lines(path, base_ref):
    absolute = ROOT / path
    current_lines = absolute.read_text(encoding="utf-8").splitlines()
    if path in _git("ls-files", "--others", "--exclude-standard").splitlines():
        return list(enumerate(current_lines, 1))
    diff = _git("diff", "--unified=0", base_ref, "--", path)
    changed = []
    line_number = 0
    for line in diff.splitlines():
        if line.startswith("@@"):
            match = re.search(r"\+(\d+)(?:,\d+)?", line)
            if match:
                line_number = int(match.group(1))
        elif line.startswith("+++") or line.startswith("---"):
            continue
        elif line.startswith("+"):
            changed.append((line_number, line[1:]))
            line_number += 1
        elif line.startswith("-"):
            continue
        elif line.startswith(" "):
            line_number += 1
    return changed


def main():
    parser = argparse.ArgumentParser(description="Reject new executable SQL outside the catalog.")
    parser.add_argument("--base-ref", default="main")
    args = parser.parse_args()

    base = _git("merge-base", args.base_ref, "HEAD").strip()
    exceptions = _exceptions()
    failures = []
    for path in _changed_paths(base):
        if path.startswith(
            ("src/containers/sql_catalog/sql/", "src/containers/sql_catalog/overrides/")
        ):
            continue
        if path.endswith(".sql") and "/dbt-projects/" in path:
            project_path = path.split("/dbt-projects/", 1)[1]
            engine, _, project_sql_path = project_path.partition("/")
            category, separator, catalog_path = project_sql_path.partition("/")
            if separator and category in {"models", "macros"}:
                model_path = catalog_path[:-4]
            else:
                category = ""
                model_path = ""
            if not category:
                failures.append(f"{path}: SQL is not at a generated dbt project path")
                continue
            try:
                generated = render_sql(engine, category, model_path)
                current = (ROOT / path).read_text(encoding="utf-8")
            except (OSError, ValueError, KeyError) as error:
                failures.append(f"{path}: cannot verify generated SQL: {error}")
                continue
            if current == generated:
                continue
            failures.append(f"{path}: differs from SQL catalog output")
            continue
        if path.endswith(".sql") and _is_exception(path, exceptions):
            continue
        if path.endswith(".sql"):
            failures.append(f"{path}: SQL added or changed outside the catalog")
            continue
        if not path.endswith(".py"):
            continue
        if _is_exception(path, exceptions):
            continue
        current_lines = (ROOT / path).read_text(encoding="utf-8").splitlines()
        for line_number, line in _added_lines(path, base):
            if is_sql_candidate(line):
                reason = _line_marker(current_lines, line_number - 1)
                if reason:
                    continue
                if reason == "":
                    failures.append(
                        f"{path}:{line_number}: SQL exemption marker requires a reason"
                    )
                    continue
                failures.append(f"{path}:{line_number}: SQL added outside the catalog")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("SQL catalog guard passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
