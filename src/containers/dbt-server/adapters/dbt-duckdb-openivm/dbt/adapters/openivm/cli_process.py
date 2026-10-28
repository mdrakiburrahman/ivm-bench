"""Observe CLI phase markers without changing SQL or retry decisions."""

import os
from pathlib import Path
import selectors
import subprocess
import tempfile
import time


def sample_process(pid, offset, reason):
    """Linux counters are diagnostic: unavailable counters never fail SQL."""
    sample = {"offset_s": offset, "reason": reason}
    paths = {"process_io": f"/proc/{pid}/io", "process_stat": f"/proc/{pid}/stat",
             "wait_channel": f"/proc/{pid}/wchan", "host_meminfo": "/proc/meminfo",
             "host_io_pressure": "/proc/pressure/io"}
    for key, path in paths.items():
        try:
            sample[key] = Path(path).read_text()
        except OSError:
            sample[key] = None
    return sample


def run_profiled_cli(args, program, markers, timeout):
    # A file-backed stdin avoids deadlocks between a large SQL program and a
    # full stdout pipe. Preparing this observer input precedes the CLI timer.
    with tempfile.TemporaryFile() as stdin:
        stdin.write(program.encode("utf-8"))
        stdin.seek(0)
        started = time.monotonic()
        deadline = started + timeout
        offsets = {}
        samples = []
        output = bytearray()
        tail = b""
        encoded = {key: (value + "\n").encode("utf-8") for key, value in markers.items()}
        keep = max(map(len, encoded.values()))
        with subprocess.Popen(args, stdin=stdin, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT) as proc:
            offsets["spawn_return_offset_s"] = time.monotonic() - started
            samples.append(sample_process(proc.pid, time.monotonic() - started, "spawn"))
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise subprocess.TimeoutExpired(args, timeout)
                        events = selector.select(min(remaining, 0.5))
                        samples.append(sample_process(proc.pid, time.monotonic() - started, "poll"))
                        if not events:
                            continue
                        chunk = os.read(proc.stdout.fileno(), 65536)
                        if not chunk:
                            samples.append(sample_process(proc.pid, time.monotonic() - started, "eof"))
                            break
                        observed = time.monotonic() - started
                        offsets.setdefault("first_response_offset_s", observed)
                        output.extend(chunk)
                        window = tail + chunk
                        for key, marker in encoded.items():
                            if marker in window:
                                if key not in offsets:
                                    offsets[key] = observed
                                    samples.append(sample_process(proc.pid, observed, key))
                        tail = window[-keep:]
                proc.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired as error:
                proc.kill()
                proc.wait()
                error.output = bytes(output)
                raise
            offsets["exit_offset_s"] = time.monotonic() - started
            offsets["process_samples"] = samples
            return (subprocess.CompletedProcess(args, proc.returncode, output.decode("utf-8")),
                    offsets["exit_offset_s"], offsets)
