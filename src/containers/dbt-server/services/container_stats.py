"""Container stats collection service — polls Docker Engine API via Unix socket."""

from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import threading
import time
import urllib.parse
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

DOCKER_SOCKET = "/var/run/docker.sock"
DEFAULT_POLL_INTERVAL = 15  # seconds
STATS_OUTPUT_DIR = os.environ.get("STATS_DIR", "/data/stats")


def summarize_cpu_seconds(
    samples: list[dict[str, Any]],
    start_ms: int,
    end_ms: int,
    included_services: list[str],
) -> dict[str, Any]:
    """Integrate sampled Docker CPU utilization over one batch window.

    Docker CPU percentage is normalized so 100% represents one fully used
    core. Each observation owns the interval halfway to its neighbours,
    clipped to the batch boundaries.
    """
    if end_ms <= start_ms:
        raise ValueError("end_ms must be greater than start_ms")

    selected = set(included_services)
    by_service: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for sample in samples:
        service = str(sample.get("container", ""))
        if service not in selected:
            continue
        try:
            timestamp_ms = float(sample["timestamp_s"]) * 1000.0
            cpu_pct = float(sample.get("cpu_pct", 0.0))
        except (KeyError, TypeError, ValueError):
            continue
        by_service[service].append((timestamp_ms, max(0.0, cpu_pct)))

    total_cpu_s = 0.0
    samples_used = 0
    service_cpu_s: dict[str, float] = {}
    for service, points in sorted(by_service.items()):
        points.sort()
        if points[0][0] > end_ms or points[-1][0] < start_ms:
            continue
        service_total = 0.0
        for index, (timestamp_ms, cpu_pct) in enumerate(points):
            left = (
                start_ms
                if index == 0
                else (points[index - 1][0] + timestamp_ms) / 2.0
            )
            right = (
                end_ms
                if index == len(points) - 1
                else (timestamp_ms + points[index + 1][0]) / 2.0
            )
            overlap_ms = max(0.0, min(right, end_ms) - max(left, start_ms))
            if overlap_ms <= 0:
                continue
            service_total += (cpu_pct / 100.0) * (overlap_ms / 1000.0)
            samples_used += 1
        if service_total > 0 or points:
            service_cpu_s[service] = service_total
            total_cpu_s += service_total

    result = {
        "status": "ok" if samples_used else "unavailable",
        "source": "docker_stats_api",
        "semantics": (
            "integral of Docker cpu_pct over the batch window; "
            "100 percent equals one utilized CPU core"
        ),
        "cpu_time_s": total_cpu_s if samples_used else None,
        "sample_count": samples_used,
        "included_services": sorted(selected),
        "service_cpu_time_s": service_cpu_s,
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
    }
    if not samples_used:
        result["error"] = "no selected container samples overlap the batch window"
    return result


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP connection over a Unix domain socket."""

    def __init__(self, socket_path: str, timeout: float = 10):
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._socket_path)


def _docker_get(path: str) -> Any:
    """Make a GET request to the Docker Engine API."""
    conn = UnixHTTPConnection(DOCKER_SOCKET)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        if resp.status != 200:
            logger.warning("Docker API %s returned %d", path, resp.status)
            return None
        return json.loads(resp.read())
    except Exception as e:
        logger.warning("Docker API request failed (%s): %s", path, e)
        return None
    finally:
        conn.close()


def _discover_containers(project: str | None = None) -> list[dict[str, str]]:
    """
    Discover running containers. If project is given, filter by
    com.docker.compose.project label. Returns list of {id, name, service}.
    """
    if project:
        filters = {"status": ["running"], "label": [f"com.docker.compose.project={project}"]}
    else:
        filters = {"status": ["running"]}

    encoded_filters = urllib.parse.quote(json.dumps(filters, separators=(",", ":")))
    data = _docker_get(f"/containers/json?filters={encoded_filters}")
    if not data:
        return []

    results = []
    for c in data:
        labels = c.get("Labels", {})
        name = (c.get("Names") or ["/unknown"])[0].lstrip("/")
        service = labels.get("com.docker.compose.service", name)
        results.append({"id": c["Id"], "name": name, "service": service})
    return results


def _parse_size_bytes(value: int | float) -> float:
    """Convert bytes to MB."""
    return round(value / (1024 * 1024), 3)


def _get_container_stats_snapshot(container_id: str) -> dict[str, Any] | None:
    """Get a one-shot stats snapshot for a container (stream=false)."""
    data = _docker_get(f"/containers/{container_id}/stats?stream=false&one-shot=true")
    if not data:
        return None

    # CPU calculation
    cpu_delta = (
        data.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
        - data.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
    )
    system_delta = (
        data.get("cpu_stats", {}).get("system_cpu_usage", 0)
        - data.get("precpu_stats", {}).get("system_cpu_usage", 0)
    )
    num_cpus = data.get("cpu_stats", {}).get("online_cpus", 1) or 1

    cpu_pct = 0.0
    if system_delta > 0 and cpu_delta >= 0:
        cpu_pct = round((cpu_delta / system_delta) * num_cpus * 100.0, 2)

    # Memory
    mem_stats = data.get("memory_stats", {})
    mem_usage_bytes = mem_stats.get("usage", 0) - mem_stats.get("stats", {}).get("cache", 0)
    mem_mb = _parse_size_bytes(max(mem_usage_bytes, 0))

    # Network
    networks = data.get("networks", {})
    net_in_bytes = sum(iface.get("rx_bytes", 0) for iface in networks.values())
    net_out_bytes = sum(iface.get("tx_bytes", 0) for iface in networks.values())

    return {
        "cpu_pct": cpu_pct,
        "mem_mb": mem_mb,
        "net_in_mb": _parse_size_bytes(net_in_bytes),
        "net_out_mb": _parse_size_bytes(net_out_bytes),
    }


class ContainerStatsCollector:
    """Background stats collector that polls all containers in a compose project."""

    def __init__(self):
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._samples: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._engine: str | None = None
        self._scale_factor: int | None = None
        self._poll_interval: float = DEFAULT_POLL_INTERVAL
        self._started_at: float | None = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def sample_count(self) -> int:
        with self._lock:
            return len(self._samples)

    def start(self, engine: str, scale_factor: int, poll_interval: float = DEFAULT_POLL_INTERVAL) -> bool:
        """Start collecting stats. Returns False if already running."""
        if self.is_running:
            return False

        self._stop_event.clear()
        self._samples = []
        self._engine = engine
        self._scale_factor = scale_factor
        self._poll_interval = poll_interval
        self._started_at = time.time()

        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("Stats collection started for engine=%s, SF=%d, interval=%ds",
                    engine, scale_factor, poll_interval)
        return True

    def stop(self) -> list[dict[str, Any]]:
        """Stop collecting and return all samples. Also writes JSONL file."""
        if not self.is_running:
            with self._lock:
                return list(self._samples)

        self._stop_event.set()
        self._thread.join(timeout=30)
        self._thread = None

        with self._lock:
            samples = list(self._samples)

        # Write JSONL output
        if samples and self._engine and self._scale_factor is not None:
            self._write_jsonl(samples)

        logger.info("Stats collection stopped. %d samples collected.", len(samples))
        return samples

    def get_status(self) -> dict[str, Any]:
        return {
            "running": self.is_running,
            "engine": self._engine,
            "scale_factor": self._scale_factor,
            "sample_count": self.sample_count,
            "poll_interval_s": self._poll_interval,
            "started_at": self._started_at,
        }

    def _poll_loop(self):
        """Main polling loop — runs in background thread."""
        # Detect compose project from our own container's labels
        project = self._detect_compose_project()

        while not self._stop_event.is_set():
            ts = time.time()
            containers = _discover_containers(project)

            for container in containers:
                stats = _get_container_stats_snapshot(container["id"])
                if stats is None:
                    continue

                sample = {
                    "timestamp_s": round(ts, 3),
                    "container": container["service"],
                    "cpu_pct": stats["cpu_pct"],
                    "mem_mb": stats["mem_mb"],
                    "net_in_mb": stats["net_in_mb"],
                    "net_out_mb": stats["net_out_mb"],
                }
                with self._lock:
                    self._samples.append(sample)

            # Wait for next interval (interruptible)
            self._stop_event.wait(timeout=self._poll_interval)

    def _detect_compose_project(self) -> str | None:
        """Detect the compose project name from this container's environment or cgroup."""
        # Docker Compose sets COMPOSE_PROJECT_NAME or we can read from /proc/1/cgroup
        project = os.environ.get("COMPOSE_PROJECT_NAME")
        if project:
            return project

        # Try reading hostname — compose containers often use service name as hostname
        # Better: inspect our own container via Docker API
        hostname = socket.gethostname()
        data = _docker_get(f"/containers/{hostname}/json")
        if data:
            labels = data.get("Config", {}).get("Labels", {})
            project = labels.get("com.docker.compose.project")
            if project:
                return project

        logger.warning("Could not detect compose project — will monitor ALL running containers")
        return None

    def _write_jsonl(self, samples: list[dict[str, Any]]):
        """Write samples to JSONL file."""
        output_dir = STATS_OUTPUT_DIR
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "container_stats.jsonl")

        try:
            with open(output_path, "w") as f:
                for sample in samples:
                    f.write(json.dumps(sample) + "\n")
            logger.info("Stats written to %s (%d samples)", output_path, len(samples))
        except OSError as e:
            logger.error("Failed to write stats file: %s", e)


# Singleton instance
collector = ContainerStatsCollector()
