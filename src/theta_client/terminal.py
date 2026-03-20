"""Manage the ThetaTerminal Java process with health checking and auto-restart."""

import logging
import subprocess
import time
from pathlib import Path

import httpx

from theta_client.requests import THETA_BASE_URL

logger = logging.getLogger(__name__)

_DEFAULT_JAR = Path(__file__).resolve().parents[2] / "ThetaTerminalv3.jar"
_HEALTH_URL = f"{THETA_BASE_URL}/stock/list/dates/quote?symbol=AAPL"


class ThetaTerminal:
    """Manages the ThetaTerminal Java process lifecycle."""

    def __init__(
        self,
        jar_path: str | Path = _DEFAULT_JAR,
        startup_timeout: float = 60.0,
        health_timeout: float = 10.0,
    ):
        self.jar_path = Path(jar_path)
        self.startup_timeout = startup_timeout
        self.health_timeout = health_timeout
        self._process: subprocess.Popen | None = None

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def health_check(self) -> bool:
        """Return True if the terminal responds to a lightweight request."""
        try:
            resp = httpx.get(_HEALTH_URL, timeout=self.health_timeout)
            return resp.status_code in (200, 472)
        except (httpx.HTTPError, httpx.ConnectError):
            return False

    def start(self) -> None:
        """Start the ThetaTerminal JAR and wait until it responds."""
        if self.running:
            logger.info("ThetaTerminal already running (pid %d)", self._process.pid)
            return

        if not self.jar_path.exists():
            raise FileNotFoundError(f"ThetaTerminal JAR not found: {self.jar_path}")

        logger.info("Starting ThetaTerminal from %s", self.jar_path)
        self._process = subprocess.Popen(
            ["java", "-jar", str(self.jar_path)],
            cwd=self.jar_path.parent,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"ThetaTerminal exited immediately with code {self._process.returncode}"
                )
            if self.health_check():
                logger.info("ThetaTerminal is ready (pid %d)", self._process.pid)
                return
            time.sleep(2.0)

        self.stop()
        raise TimeoutError(
            f"ThetaTerminal did not become healthy within {self.startup_timeout}s"
        )

    def stop(self) -> None:
        """Stop the ThetaTerminal process."""
        if self._process is None:
            return
        logger.info("Stopping ThetaTerminal (pid %d)", self._process.pid)
        self._process.terminate()
        try:
            self._process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            logger.warning("ThetaTerminal did not stop gracefully, killing")
            self._process.kill()
            self._process.wait()
        self._process = None

    def restart(self) -> None:
        """Stop and start the terminal."""
        self.stop()
        time.sleep(2.0)
        self.start()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
