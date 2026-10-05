"""Read existing benchmark sessions only; never save credentials or raw logs."""
import base64
import importlib.util
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

TARGETS = {
    "a660da51-24b8-4c94-aae2-c534af5e9b9c": "figure1-36248895920",
    "108f657d-2da7-45c7-a220-4d8de50cc513": "faulty-37236087660",
    "2b016093-f793-4e61-b65f-368f62f5ffd1": "latest-37306619326",
}
FIELDS = {
    "session": "state runtimeVersion submittedDateTime startDateTime endDateTime driverMemory driverCores executorMemory executorCores numExecutors isDynamicAllocationEnabled attemptNumber".split(),
    "executors": "id isActive totalCores maxMemory totalTasks totalDuration totalGCTime totalInputBytes totalShuffleRead totalShuffleWrite failedTasks completedTasks".split(),
    "stages": "stageId attemptId status submissionTime completionTime numTasks numFailedTasks executorRunTime executorCpuTime jvmGcTime inputBytes outputBytes shuffleReadBytes shuffleWriteBytes memoryBytesSpilled diskBytesSpilled".split(),
}


def pick(record, fields):
    return {key: record[key] for key in fields if key in record}


def control_session(session, since):
    return (
        session.get("itemType") == "Lakehouse"
        and bool(re.fullmatch(r"openivm_jvm_35_[0-9]+_[a-z0-9]+", session.get("itemName", "")))
        and session.get("submittedDateTime", "") >= since
    )


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def control_finished():
    # Warm-up sessions can finish before dbt starts; the GCI run, rather than
    # any individual Spark session, is the authoritative stopping condition.
    request = urllib.request.Request(
        "https://api.github.com/repos/mdrakiburrahman/ivm-bench/actions/runs/37382082873",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=15) as response:
            return json.load(response).get("status") == "completed"
    except (OSError, ValueError):
        return False


def run():
    allowed = {"IMDS_RELAY_URL", "IMDS_RELAY_SENDER_KEY", "IMDS_RELAY_KEY_NAME", "UAMI_CLIENT_ID", "FABRIC_API_BASE", "FABRIC_WORKSPACE_ID"}
    for line in base64.b64decode(os.environ["BASE64_ENV"]).decode().splitlines():
        key, separator, value = line.partition("=")
        key = key.strip().removeprefix("export ")
        if separator and key in allowed:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ[key] = value
    base = os.environ.get("FABRIC_API_BASE", "https://api.fabric.microsoft.com").rstrip("/")
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme != "https" or parsed.hostname != "api.fabric.microsoft.com":
        raise ValueError("Unsupported API host")
    workspace = os.environ["FABRIC_WORKSPACE_ID"]
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", workspace):
        raise ValueError("Invalid workspace ID")
    spec = importlib.util.spec_from_file_location("relay", "src/containers/imds-router/imds_relay_router.py")
    relay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(relay)
    token, error = relay._relay_fetch("https://api.fabric.microsoft.com", os.environ.get("UAMI_CLIENT_ID", ""))
    if error or not token:
        raise RuntimeError("Token unavailable")
    opener = urllib.request.build_opener(NoRedirect)

    def get(path, params=None, text=False):
        url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
        request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token["access_token"]})
        try:
            with opener.open(request, timeout=30) as response:
                payload = response.read(16 * 1024 * 1024 + 1)
                if len(payload) > 16 * 1024 * 1024:
                    return {"unavailable": "response-size-limit"}
                return payload.decode(errors="replace") if text else json.loads(payload)
        except urllib.error.HTTPError as exc:
            return {"unavailable": "http-" + str(exc.code)}
        except (OSError, ValueError):
            return {"unavailable": "request-or-decode-error"}

    report = {label: {"availability": "not-found"} for label in TARGETS.values()}
    control_since = os.environ.get("CONTROL_SINCE", "")
    targets = dict(TARGETS)
    controls = []
    continuation = None
    for page in range(20):
        listing = get(f"/v1/workspaces/{workspace}/spark/livySessions", {"continuationToken": continuation} if continuation else None)
        if "unavailable" in listing:
            report["listing"] = listing
            break
        for session in listing.get("value", []):
            item = session.get("item", {}).get("itemId")
            if not isinstance(item, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", item):
                continue
            if control_since:
                if not control_session(session, control_since):
                    continue
                controls.append(item)
                targets[item] = "control-37382082873"
                report.setdefault(targets[item], {"availability": "not-found"})
            if item not in targets or (control_since and item not in controls):
                continue
            livy, app = session.get("livyId", ""), session.get("sparkApplicationId", "")
            if not re.fullmatch(r"[0-9a-fA-F-]{36}", livy) or not re.fullmatch(r"application_[0-9_]+", app):
                continue
            root = f"/v1/workspaces/{workspace}/lakehouses/{item}/livySessions/{livy}"
            result = {"session": pick(session, FIELDS["session"])}
            details = get(root)
            result["details"] = {"unavailable": details["unavailable"]} if "unavailable" in details else pick(details, FIELDS["session"])
            app_root = root + "/applications/" + app
            for endpoint in ("executors", "stages"):
                metrics = get(app_root + "/" + endpoint)
                result[endpoint] = [pick(row, FIELDS[endpoint]) for row in metrics] if isinstance(metrics, list) else {"unavailable": metrics.get("unavailable", "unexpected-shape")}
            environment = get(app_root + "/environment")
            config_keys = {"spark.executor.cores", "spark.executor.memory", "spark.executor.instances", "spark.dynamicAllocation.enabled", "spark.dynamicAllocation.minExecutors", "spark.dynamicAllocation.maxExecutors", "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold"}
            result["sparkProperties"] = {key: value for key, value in environment.get("sparkProperties", []) if key in config_keys}
            # A live observer samples metrics only. Avoid rereading driver logs
            # every minute; retrieve signatures after the session completes.
            logs = {"unavailable": "deferred-until-terminal"}
            if not control_since or session.get("state") in ("Succeeded", "Failed", "Cancelled"):
                logs = get(app_root + "/logs", {"type": "driver", "fileName": "stderr", "isDownload": "true"}, text=True)
            if isinstance(logs, str):
                result["stderr_signatures"] = {
                    "GLIBCXX_versions": sorted(set(re.findall(r"GLIBCXX_[0-9.]+", logs))),
                    "native_compile_failures": len(re.findall(r"(?i)native compiler failed|compile_failed", logs)),
                    "backup_pass_counts": [int(n) for n in re.findall(r"state-sync: backed up (\d+) files", logs)],
                }
            else:
                result["stderr_signatures"] = {"unavailable": logs.get("unavailable", "unexpected-shape")}
            result["itemId"] = item
            report[targets[item]].setdefault("sessions", []).append(result)
            report[targets[item]]["availability"] = "listed"
        continuation = listing.get("continuationToken")
        if not continuation:
            break
    else:
        report["listing"] = {"unavailable": "page-limit"}
    if control_since:
        report = {"control-37382082873": report.get("control-37382082873", {"availability": "not-found"}), "listing": report.get("listing", {})}
        if len(set(controls)) > 1:
            report = {"unavailable": "ambiguous-control-items"}
    return report


if __name__ == "__main__":
    control = bool(os.environ.get("CONTROL_SINCE"))
    deadline = time.monotonic() + (140 * 60 if control else 0)
    last_available = None
    while True:
        try:
            output = run()
        except Exception as exc:
            # Messages may contain relay URLs or tokens. Persist only exception type.
            output = {"unavailable": type(exc).__name__}
        result = output.get("control-37382082873", {})
        sessions = result.get("sessions", [])
        if any(isinstance(row.get("executors"), list) and row["executors"] for row in sessions):
            last_available = output
        Path("fabric-history-diagnostic.json").write_text(json.dumps(last_available or output, indent=2))
        terminal = control_finished() if control else True
        print(json.dumps({"observed": result.get("availability"), "terminal": terminal, "metrics_retained": last_available is not None}), flush=True)
        if not control or terminal or time.monotonic() >= deadline or output.get("unavailable") == "ambiguous-control-items":
            break
        time.sleep(60)
