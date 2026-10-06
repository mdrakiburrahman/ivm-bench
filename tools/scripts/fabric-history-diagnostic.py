"""Read existing benchmark sessions only; never save credentials or raw logs."""
import base64
import datetime
import importlib.util
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CONTROL_RUN_ID = os.environ.get("CONTROL_RUN_ID", "37382082873")
if not re.fullmatch(r"[1-9][0-9]*", CONTROL_RUN_ID):
    raise ValueError("Invalid control run ID")
CONTROL_LABEL = "control-" + CONTROL_RUN_ID

TARGETS = {
    "a660da51-24b8-4c94-aae2-c534af5e9b9c": "figure1-36248895920",
    "108f657d-2da7-45c7-a220-4d8de50cc513": "faulty-37236087660",
    "2b016093-f793-4e61-b65f-368f62f5ffd1": "latest-37306619326",
}
FIELDS = {
    "session": "state runtimeVersion submittedDateTime startDateTime endDateTime driverMemory driverCores executorMemory executorCores numExecutors isDynamicAllocationEnabled attemptNumber".split(),
    "executors": "id isActive addTime removeTime totalCores maxMemory totalTasks totalDuration totalGCTime totalInputBytes totalShuffleRead totalShuffleWrite failedTasks completedTasks".split(),
    "stages": "stageId attemptId status submissionTime completionTime numTasks numFailedTasks executorRunTime executorCpuTime jvmGcTime inputBytes outputBytes shuffleReadBytes shuffleWriteBytes memoryBytesSpilled diskBytesSpilled".split(),
}


def pick(record, fields):
    return {key: record[key] for key in fields if key in record}


def workspace_compute_summary(settings, pools):
    """Persist compute limits only; omit arbitrary workspace and pool names."""
    if "unavailable" in settings:
        return {"settings": settings}
    pool = settings.get("pool", {})
    default = pool.get("defaultPool", {})
    summary = {
        "settings": {
            "pool": {
                "customizeComputeEnabled": pool.get("customizeComputeEnabled"),
                "defaultPoolType": default.get("type"),
                "isStarterPool": default.get("name") == "Starter Pool",
                "starterPool": pick(pool.get("starterPool", {}), ["maxNodeCount", "maxExecutors"]),
            },
            "environment": pick(settings.get("environment", {}), ["runtimeVersion"]),
            "job": pick(settings.get("job", {}), ["conservativeJobAdmissionEnabled", "sessionTimeoutInMinutes"]),
        },
    }
    if "unavailable" in pools:
        summary["pools"] = pools
    else:
        summary["pools"] = [{
            **pick(row, ["type", "nodeFamily", "nodeSize"]),
            "isDefault": row.get("id") == default.get("id"),
            "isStarterPool": row.get("name") == "Starter Pool",
            "autoScale": pick(row.get("autoScale", {}), ["enabled", "minNodeCount", "maxNodeCount"]),
            "dynamicExecutorAllocation": pick(row.get("dynamicExecutorAllocation", {}), ["enabled", "minExecutors", "maxExecutors"]),
        } for row in pools.get("value", [])]
        summary["pools_truncated"] = bool(pools.get("continuationToken"))
    return summary


def sql_summary(record, model_names):
    result = pick(record, "id status submissionTime duration runningJobIds successJobIds failedJobIds".split())
    text = str(record.get("description", "")) + "\n" + str(record.get("planDescription", ""))
    result["models_mentioned"] = sorted(name for name in model_names if re.search(r"\b" + re.escape(name) + r"\b", text))
    operators = ("AdaptiveSparkPlan", "WriteFiles", "SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "HashAggregate", "Exchange", "Window", "InMemoryTableScan", "ColumnarToRow", "RowToColumnar")
    result["operator_counts"] = {name: count for name in operators if (count := len(re.findall(r"\b" + name + r"\b", text)))}
    return result


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
        "https://api.github.com/repos/mdrakiburrahman/ivm-bench/actions/runs/" + CONTROL_RUN_ID,
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

    if os.environ.get("SETTINGS_ONLY") == "true":
        return workspace_compute_summary(
            get(f"/v1/workspaces/{workspace}/spark/settings"),
            get(f"/v1/workspaces/{workspace}/spark/pools"),
        )

    report = {label: {"availability": "not-found"} for label in TARGETS.values()}
    control_since = os.environ.get("CONTROL_SINCE", "")
    targets = dict(TARGETS)
    controls = []
    listing_path = f"/v1/workspaces/{workspace}/spark/livySessions"
    control_item = None
    control_discovery = {}
    if control_since:
        since_epoch = datetime.datetime.fromisoformat(control_since.replace("Z", "+00:00")).timestamp()
        continuation = None
        candidates = []
        for page in range(20):
            items = get(f"/v1/workspaces/{workspace}/lakehouses", {"continuationToken": continuation} if continuation else None)
            if "unavailable" in items:
                control_discovery = {"unavailable": items["unavailable"]}
                break
            for item in items.get("value", []):
                match = re.fullmatch(r"openivm_jvm_35_([0-9]+)_[a-z0-9]+", item.get("displayName", ""))
                if match and int(match[1]) / 1_000_000 >= since_epoch:
                    if re.fullmatch(r"[0-9a-fA-F-]{36}", item.get("id", "")):
                        candidates.append(item)
            continuation = items.get("continuationToken")
            if not continuation:
                control_discovery = {"matching_lakehouses": len(candidates)}
                break
        else:
            control_discovery = {"unavailable": "page-limit"}
        if len(candidates) > 1:
            return {"unavailable": "ambiguous-control-items"}
        if candidates:
            control_item = candidates[0]
            listing_path = f"/v1/workspaces/{workspace}/lakehouses/{control_item['id']}/livySessions"
    continuation = None
    for page in range(20):
        listing = get(listing_path, {"continuationToken": continuation} if continuation else None)
        if "unavailable" in listing:
            report["listing"] = listing
            break
        for session in listing.get("value", []):
            if control_item:
                session = dict(session, item={"itemId": control_item["id"]}, itemName=control_item["displayName"], itemType="Lakehouse")
            item = session.get("item", {}).get("itemId")
            if not isinstance(item, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", item):
                continue
            if control_since:
                if not control_session(session, control_since):
                    continue
                controls.append(item)
                targets[item] = CONTROL_LABEL
                report.setdefault(targets[item], {"availability": "not-found"})
            if item not in targets or (control_since and item not in controls):
                continue
            livy, app = session.get("livyId", ""), session.get("sparkApplicationId", "")
            if not re.fullmatch(r"[0-9a-fA-F-]{36}", livy) or not re.fullmatch(r"application_[0-9_]+", app):
                continue
            root = f"/v1/workspaces/{workspace}/lakehouses/{item}/livySessions/{livy}"
            result = {"session": pick(session, FIELDS["session"]), "applicationId": app}
            details = get(root)
            result["details"] = {"unavailable": details["unavailable"]} if "unavailable" in details else pick(details, FIELDS["session"])
            app_root = root + "/applications/" + app
            for endpoint in ("executors", "stages"):
                metrics = get(app_root + "/" + endpoint)
                result[endpoint] = [pick(row, FIELDS[endpoint]) for row in metrics] if isinstance(metrics, list) else {"unavailable": metrics.get("unavailable", "unexpected-shape")}
            # /executors lists active executors; retain removed ones as well
            # to distinguish an allocation limit from later scale-down.
            metrics = get(app_root + "/allexecutors")
            result["allExecutors"] = [pick(row, FIELDS["executors"]) for row in metrics] if isinstance(metrics, list) else {"unavailable": metrics.get("unavailable", "unexpected-shape")}
            if os.environ.get("OBSERVE_ONCE") == "true":
                # One-off snapshots only: do not add query-list traffic to the
                # continuous monitor. Persist no raw SQL, plans or job names.
                names = {path.stem for path in Path("src/containers/dbt-server/dbt-projects/spark-openivm/models").rglob("*.sql")}
                queries = get(app_root + "/sql", {"offset": 0, "length": 1000, "details": "false", "planDescription": "true"})
                result["sql"] = [sql_summary(row, names) for row in queries] if isinstance(queries, list) else {"unavailable": queries.get("unavailable", "unexpected-shape")}
                jobs = get(app_root + "/jobs", {"offset": 0, "length": 1000})
                result["jobs"] = [pick(row, "jobId status submissionTime completionTime stageIds numTasks numFailedTasks".split()) for row in jobs] if isinstance(jobs, list) else {"unavailable": jobs.get("unavailable", "unexpected-shape")}
            environment = get(app_root + "/environment")
            config_keys = {"spark.executor.cores", "spark.executor.memory", "spark.executor.instances", "spark.dynamicAllocation.enabled", "spark.dynamicAllocation.minExecutors", "spark.dynamicAllocation.maxExecutors", "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold"}
            config_keys.update({
                "spark.dynamicAllocation.executorAllocationRatio", "spark.dynamicAllocation.executorIdleTimeout", "spark.dynamicAllocation.schedulerBacklogTimeout",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes", "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.files.maxPartitionBytes",
                "spark.sql.parquet.compression.codec", "spark.sql.parquet.vorder.default", "spark.fabric.resourceProfile",
                "spark.databricks.delta.optimizeWrite.enabled", "spark.databricks.delta.autoCompact.enabled", "spark.databricks.delta.autoCompact.minNumFiles",
                "spark.openivm.delta.optimizeWrite", "spark.openivm.delta.autoCompact",
            })
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
        report = {CONTROL_LABEL: report.get(CONTROL_LABEL, {"availability": "not-found"}), "listing": report.get("listing", {}), "discovery": control_discovery}
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
        result = output.get(CONTROL_LABEL, {})
        sessions = result.get("sessions", [])
        if any(isinstance(row.get("executors"), list) and row["executors"] for row in sessions):
            last_available = output
        Path("fabric-history-diagnostic.json").write_text(json.dumps(last_available or output, indent=2))
        # Keep every redacted snapshot: Spark evicts completed stages, so the
        # last snapshot alone cannot explain earlier expensive writes.
        with Path("fabric-history-diagnostic-snapshots.jsonl").open("a") as snapshots:
            snapshots.write(json.dumps({"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "report": output}) + "\n")
        terminal = control_finished() if control else True
        print(json.dumps({"observed": result.get("availability"), "terminal": terminal, "metrics_retained": last_available is not None}), flush=True)
        if os.environ.get("OBSERVE_ONCE") == "true" or not control or terminal or time.monotonic() >= deadline or output.get("unavailable") == "ambiguous-control-items":
            break
        time.sleep(60)
