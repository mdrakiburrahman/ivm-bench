"""Docker Compose manager — OOP wrapper for the docker compose CLI."""

import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class DockerManager:
    """
    Manages Docker Compose lifecycle for benchmark orchestration.

    Wraps the ``docker compose`` CLI with environment injection,
    real-time log streaming, and health checking.
    """

    def __init__(
        self,
        compose_file: str,
        project_name: str = "benchmark",
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        extra_compose_files: Optional[List[str]] = None,
    ) -> None:
        self._compose_file = str(Path(compose_file).resolve())
        self._project_name = project_name
        self._env = self._build_env(env)
        self._cwd = cwd or str(Path(compose_file).resolve().parent)
        self._extra_compose_files = extra_compose_files or []

    def _build_env(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Merge OS environment with extra vars."""
        env = dict(os.environ)
        if extra:
            env.update(extra)
        return env

    def update_env(self, extra: Dict[str, str]) -> None:
        """Add or overwrite environment variables."""
        self._env.update(extra)

    @property
    def _base_cmd(self) -> List[str]:
        cmd = [
            "docker", "compose",
            "-f", self._compose_file,
        ]
        for f in self._extra_compose_files:
            cmd.extend(["-f", f])
        cmd.extend(["-p", self._project_name])
        return cmd

    def _run(
        self,
        args: List[str],
        check: bool = True,
        timeout: Optional[int] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> subprocess.CompletedProcess:
        """Run a docker compose command."""
        cmd = self._base_cmd + args
        logger.info("Running: %s", " ".join(cmd))

        if stream_callback:
            return self._run_streaming(cmd, check, timeout, stream_callback)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=self._env,
            cwd=self._cwd,
        )
        if result.stdout:
            logger.debug("stdout: %s", result.stdout[:1000])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[:1000])
        if check and result.returncode != 0:
            raise RuntimeError(
                f"Docker compose command failed (exit {result.returncode}):\n"
                f"cmd: {' '.join(cmd)}\n"
                f"stderr: {result.stderr[:2000]}"
            )
        return result

    def _run_streaming(
        self,
        cmd: List[str],
        check: bool,
        timeout: Optional[int],
        callback: Callable[[str], None],
    ) -> subprocess.CompletedProcess:
        """Run a command and stream output line-by-line via callback."""
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=self._env,
            cwd=self._cwd,
        )
        output_lines: List[str] = []

        def _reader():
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                output_lines.append(line)
                callback(line)

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        reader.join(timeout=5)

        stdout = "\n".join(output_lines)
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"Docker compose command failed (exit {proc.returncode}):\n"
                f"cmd: {' '.join(cmd)}\n"
                f"output: {stdout[:2000]}"
            )
        return subprocess.CompletedProcess(cmd, proc.returncode, stdout, "")

    # ----- High-level operations -----

    def build(self, services: Optional[List[str]] = None, timeout: int = 3600) -> None:
        """Build images."""
        args = ["build"]
        if services:
            args.extend(services)
        self._run(args, timeout=timeout)
        logger.info("Build completed for %s", services or "all services")

    def up(
        self,
        services: Optional[List[str]] = None,
        detach: bool = True,
        wait: bool = False,
        timeout: int = 600,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Start containers."""
        args = ["up"]
        if detach:
            args.append("-d")
        if wait:
            args.append("--wait")
            args.extend(["--wait-timeout", str(timeout)])
        if services:
            args.extend(services)
        self._run(args, timeout=timeout + 60, stream_callback=stream_callback)
        logger.info("Up completed")

    def down(self, volumes: bool = False, timeout: int = 120) -> None:
        """Stop and remove containers."""
        args = ["down", "--remove-orphans"]
        if volumes:
            args.append("-v")
        self._run(args, check=False, timeout=timeout)
        logger.info("Down completed")

    def run_service(
        self,
        service: str,
        cmd_args: Optional[List[str]] = None,
        timeout: int = 1800,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> subprocess.CompletedProcess:
        """Run a one-shot service (docker compose run --rm)."""
        args = ["run", "--rm", service]
        if cmd_args:
            args.extend(cmd_args)
        return self._run(args, timeout=timeout, stream_callback=stream_callback)

    def logs(self, service: Optional[str] = None, tail: int = 200) -> str:
        """Retrieve container logs."""
        args = ["logs", "--no-color", "--timestamps", "--tail", str(tail)]
        if service:
            args.append(service)
        result = self._run(args, check=False)
        return result.stdout + result.stderr

    def ps(self, service: Optional[str] = None) -> str:
        """List running containers."""
        args = ["ps", "--format", "json"]
        if service:
            args.append(service)
        result = self._run(args, check=False)
        return result.stdout

    def is_healthy(self, service: str) -> bool:
        """Check if a service container reports healthy."""
        output = self.ps(service)
        return "healthy" in output.lower()

    def wait_for_healthy(
        self,
        service: str,
        timeout: int = 300,
        interval: int = 5,
    ) -> None:
        """Poll until a service is healthy."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_healthy(service):
                logger.info("Service '%s' is healthy", service)
                return
            logger.debug("Waiting for '%s'...", service)
            time.sleep(interval)
        raise TimeoutError(f"Service '{service}' not healthy within {timeout}s")

    def capture_all_logs(self, dest_dir: str) -> None:
        """Save per-service logs to files in dest_dir."""
        os.makedirs(dest_dir, exist_ok=True)
        ps_output = self._run(["ps", "--services"], check=False)
        services = [s.strip() for s in ps_output.stdout.splitlines() if s.strip()]
        for svc in services:
            log_text = self.logs(svc, tail=10000)
            log_path = os.path.join(dest_dir, f"{svc}.log")
            with open(log_path, "w") as f:
                f.write(log_text)
        logger.info("Logs captured to %s", dest_dir)

    def get_exit_code(self, service: str) -> str:
        """Get exit code for a completed service."""
        result = self._run(
            ["ps", "-a", service, "--format", "{{.ExitCode}}"],
            check=False,
        )
        return result.stdout.strip() or "unknown"

    def exec_in(self, service: str, cmd_args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Execute a command inside a running service container."""
        return self._run(["exec", "-T", service] + cmd_args, check=check)
