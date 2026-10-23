"""Observe CLI phase markers without changing SQL or retry decisions."""

import os
import selectors
import subprocess
import tempfile
import time


def run_profiled_cli(args, program, markers, timeout):
    # A file-backed stdin avoids deadlocks between a large SQL program and a
    # full stdout pipe. Preparing this observer input precedes the CLI timer.
    with tempfile.TemporaryFile() as stdin:
        stdin.write(program.encode("utf-8"))
        stdin.seek(0)
        started = time.monotonic()
        deadline = started + timeout
        offsets = {}
        output = bytearray()
        tail = b""
        encoded = {key: (value + "\n").encode("utf-8") for key, value in markers.items()}
        keep = max(map(len, encoded.values()))
        with subprocess.Popen(args, stdin=stdin, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT) as proc:
            offsets["spawn_return_offset_s"] = time.monotonic() - started
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not selector.select(remaining):
                            raise subprocess.TimeoutExpired(args, timeout)
                        chunk = os.read(proc.stdout.fileno(), 65536)
                        if not chunk:
                            break
                        observed = time.monotonic() - started
                        offsets.setdefault("first_response_offset_s", observed)
                        output.extend(chunk)
                        window = tail + chunk
                        for key, marker in encoded.items():
                            if marker in window:
                                offsets.setdefault(key, observed)
                        tail = window[-keep:]
                proc.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired as error:
                proc.kill()
                proc.wait()
                error.output = bytes(output)
                raise
            offsets["exit_offset_s"] = time.monotonic() - started
            return (subprocess.CompletedProcess(args, proc.returncode, output.decode("utf-8")),
                    offsets["exit_offset_s"], offsets)
