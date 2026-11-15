import argparse
import fnmatch
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTAINERS_DIR = ROOT / "src" / "containers"
EXCEPTIONS_PATH = CONTAINERS_DIR / "sql_catalog" / "exceptions.json"
sys.path.insert(0, str(CONTAINERS_DIR))

from sql_catalog.generator import is_sql_candidate, render_sql


def _git(*args):
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _exceptions():
    return json.loads(EXCEPTIONS_PATH.read_text(encoding="utf-8"))


def _is_exception(path, exceptions):
    return any(fnmatch.fnmatch(path, item["path"]) for item in exceptions)


def _changed_paths(base_ref):
    tracked = set(_git("diff", "--name-only", f"{base_ref}...HEAD").splitlines())
    tracked.update(
        _git("diff", "--name-only", "--").splitlines()
    )
    untracked = _git("ls-files", "--others", "--exclude-standard").splitlines()
    tracked.update(untracked)
    return sorted(path for path in tracked if path)


def _added_lines(path, base_ref):
    absolute = ROOT / path
    if path in _git("ls-files", "--others", "--exclude-standard").splitlines():
        return absolute.read_text(encoding="utf-8").splitlines()
    diff = _git("diff", "--unified=0", f"{base_ref}...", "--", path)
    return [line[1:] for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]


def main():
    parser = argparse.ArgumentParser(description="Reject new executable SQL outside the catalog.")
    parser.add_argument("--base-ref", default="main")
    args = parser.parse_args()

    base = _git("merge-base", args.base_ref, "HEAD").strip()
    exceptions = _exceptions()
    failures = []
    for path in _changed_paths(base):
        if path.startswith("src/containers/sql_catalog/sql/"):
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
        for line_number, line in enumerate(_added_lines(path, base), 1):
            if is_sql_candidate(line):
                failures.append(f"{path}:{line_number}: SQL added outside the catalog")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("SQL catalog guard passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
