#!/usr/bin/env python3
"""Small dependency-free Safari/WebDriver receipt for the hosted private-mode probe.

It records page-visible lifecycle facts and IndexedDB *metadata only*. It never
collects cookies, local/session storage, IndexedDB names, keys, or values.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import platform
import re
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


class WebDriverError(RuntimeError):
    pass


class Driver:
    def __init__(self, endpoint: str, out: Path):
        self.endpoint = endpoint.rstrip("/")
        self.out = out
        self.session: str | None = None
        self.capabilities: dict[str, Any] = {}

    def request(self, method: str, path: str, payload: Any | None = None, timeout: int = 30) -> Any:
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.endpoint + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json"} if body else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                result = json.loads(response.read().decode() or "{}")
        except urllib.error.HTTPError as error:
            detail = error.read().decode(errors="replace")
            raise WebDriverError(f"{method} {path} HTTP {error.code}: {detail}") from error
        except OSError as error:
            raise WebDriverError(f"{method} {path}: {error}") from error
        if isinstance(result, dict) and result.get("value", {}).get("error"):
            raise WebDriverError(f"{method} {path}: {result['value']}")
        return result.get("value") if isinstance(result, dict) and "value" in result else result

    def start(self) -> None:
        value = self.request(
            "POST",
            "/session",
            {"capabilities": {"alwaysMatch": {"browserName": "safari"}}},
            timeout=60,
        )
        if not isinstance(value, dict):
            raise WebDriverError(f"unexpected session response: {value!r}")
        self.session = value.get("sessionId")
        self.capabilities = value.get("capabilities") or {}
        if not self.session:
            raise WebDriverError(f"session response had no sessionId: {value!r}")
        name = str(self.capabilities.get("browserName", "")).lower()
        version = self.capabilities.get("browserVersion") or self.capabilities.get("version")
        if name != "safari" or not version:
            raise WebDriverError(f"expected Safari with a version, got {self.capabilities!r}")

    def close(self) -> None:
        if self.session:
            try:
                self.request("DELETE", f"/session/{self.session}")
            finally:
                self.session = None

    def navigate(self, url: str) -> None:
        self.request("POST", f"/session/{self.session}/url", {"url": url}, timeout=60)

    def refresh(self) -> None:
        self.request("POST", f"/session/{self.session}/refresh", {}, timeout=60)

    def execute(self, script: str, args: list[Any] | None = None) -> Any:
        return self.request(
            "POST", f"/session/{self.session}/execute/sync", {"script": script, "args": args or []}
        )

    def execute_async(self, script: str, args: list[Any] | None = None) -> Any:
        return self.request(
            "POST", f"/session/{self.session}/execute/async", {"script": script, "args": args or []}, timeout=30
        )

    def screenshot(self, name: str) -> None:
        try:
            encoded = self.request("GET", f"/session/{self.session}/screenshot", timeout=30)
            (self.out / f"{name}.png").write_bytes(base64.b64decode(encoded))
        except Exception as error:  # diagnostic best effort
            print(f"screenshot failure ({name}): {error}", file=sys.stderr)


STATUS = r"""
const text = (id) => document.getElementById(id)?.textContent?.trim() ?? null;
const hidden = (id) => Boolean(document.getElementById(id)?.hidden);
return {
  readyStatus: text('receipt-status'),
  startupError: hidden('startup-error') ? null : text('startup-error'),
  applicationError: hidden('error-message') ? null : text('error-message'),
  restartEnabled: !Boolean(document.getElementById('restart-storage')?.disabled),
  addEnabled: !Boolean(document.querySelector('#add-form button[type="submit"]')?.disabled),
  markerVisible: document.body?.innerText?.includes(arguments[0]) ?? false,
  documentReadyState: document.readyState,
};
"""

IDB_METADATA = r"""
const done = arguments[arguments.length - 1];
try {
  if (typeof indexedDB.databases !== 'function') {
    done({ available: false, count: null, versions: [] });
  } else {
    indexedDB.databases().then(
      (dbs) => done({
        available: true,
        count: dbs.length,
        // Deliberately omit database names: this receipt needs counts only.
        versions: dbs.map((db) => db.version ?? null),
      }),
      (error) => done({ available: true, error: String(error) }),
    );
  }
} catch (error) { done({ available: false, error: String(error) }); }
"""

ADD_MARKER = r"""
const marker = arguments[0];
const input = document.getElementById('title-input');
const form = document.getElementById('add-form');
if (!(input instanceof HTMLInputElement) || !(form instanceof HTMLFormElement)) {
  throw new Error('hosted Safari receipt UI is missing #title-input or #add-form');
}
input.value = marker;
input.dispatchEvent(new Event('input', { bubbles: true }));
form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
return true;
"""


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def event(events: list[dict[str, Any]], phase: str, **details: Any) -> None:
    entry = {"at": now(), "phase": phase, **details}
    events.append(entry)
    print(json.dumps(entry, sort_keys=True), flush=True)


def wait_for(driver: Driver, marker: str, seconds: float, events: list[dict[str, Any]], phase: str) -> dict[str, Any]:
    deadline = time.monotonic() + seconds
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        value = driver.execute(STATUS, [marker])
        latest = value if isinstance(value, dict) else {"raw": value}
        if latest.get("startupError") or latest.get("applicationError"):
            raise WebDriverError(f"{phase}: page reported error: {latest}")
        if latest.get("markerVisible"):
            event(events, phase, result="marker-visible", status=latest)
            return latest
        time.sleep(0.5)
    raise WebDriverError(f"{phase}: marker not visible after {seconds}s; final status={latest}")


def snapshot(driver: Driver, marker: str, events: list[dict[str, Any]], phase: str) -> None:
    status = driver.execute(STATUS, [marker])
    idb = driver.execute_async(IDB_METADATA)
    event(events, phase, status=status, indexedDbMetadata=idb)
    if isinstance(status, dict) and (status.get("startupError") or status.get("applicationError")):
        raise WebDriverError(f"{phase}: page reported error: {status}")


def wait_ready(driver: Driver, marker: str, events: list[dict[str, Any]], phase: str) -> None:
    deadline = time.monotonic() + 20
    latest: Any = None
    while time.monotonic() < deadline:
        latest = driver.execute(STATUS, [marker])
        if isinstance(latest, dict) and (latest.get("startupError") or latest.get("applicationError")):
            raise WebDriverError(f"{phase}: page reported error: {latest}")
        if isinstance(latest, dict) and latest.get("restartEnabled") and latest.get("addEnabled"):
            event(events, phase, result="ready", status=latest)
            return
        time.sleep(0.5)
    raise WebDriverError(f"{phase}: app was not ready after 20s; final status={latest}")


def version(command: list[str]) -> str | None:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT, timeout=15).strip()
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4444")
    parser.add_argument("--output", default="safari-webdriver-receipt")
    args = parser.parse_args()
    if not 1 <= args.cycles <= 12:
        parser.error("--cycles must be 1..12")

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, Any]] = []
    driver = Driver(args.endpoint, out)
    receipt: dict[str, Any] = {
        "startedAt": now(),
        "url": args.url,
        "cycles": args.cycles,
        "host": {"platform": platform.platform(), "safari": version(["/Applications/Safari.app/Contents/MacOS/Safari", "--version"]), "safaridriver": version(["/usr/bin/safaridriver", "--version"])},
        "events": events,
    }
    try:
        event(events, "webdriver-session-start")
        driver.start()
        receipt["capabilities"] = driver.capabilities
        event(events, "webdriver-session-ready", browserName=driver.capabilities.get("browserName"), browserVersion=driver.capabilities.get("browserVersion") or driver.capabilities.get("version"))
        driver.navigate(args.url)
        event(events, "initial-navigation-complete")
        marker_prefix = f"safari-e2e-{int(time.time())}"
        wait_ready(driver, marker_prefix, events, "initial-ready")
        snapshot(driver, marker_prefix, events, "initial-diagnostics")

        for cycle in range(args.cycles):
            marker = f"{marker_prefix}-{cycle}"
            event(events, "write-dispatch", cycle=cycle, marker=marker)
            driver.execute(ADD_MARKER, [marker])
            wait_for(driver, marker, 10, events, "immediate-visible")
            snapshot(driver, marker, events, "after-immediate-visible")
            time.sleep(5)
            wait_for(driver, marker, 1, events, "five-second-visible")
            snapshot(driver, marker, events, "after-five-second-delay")

            # This is a UI-visible diagnostic local-ack/reopen flow: the hosted
            # probe calls Db.shutdown then recreates its persistent runtime.
            driver.execute("document.getElementById('restart-storage')?.click(); return true;")
            event(events, "diagnostic-local-ack-restart-clicked", cycle=cycle)
            wait_ready(driver, marker, events, "diagnostic-local-ack-ready")
            wait_for(driver, marker, 20, events, "diagnostic-local-ack-marker")

            driver.refresh()
            event(events, "reload-complete", cycle=cycle)
            wait_ready(driver, marker, events, "post-reload-ready")
            wait_for(driver, marker, 20, events, "post-reload-marker")
            snapshot(driver, marker, events, "post-reload-diagnostics")

        receipt["result"] = "passed"
        event(events, "receipt-passed")
        return 0
    except Exception as error:
        receipt["result"] = "failed"
        receipt["failure"] = {"type": type(error).__name__, "message": str(error), "traceback": traceback.format_exc()}
        event(events, "receipt-failed", error=str(error))
        driver.screenshot("failure")
        return 1
    finally:
        receipt["finishedAt"] = now()
        try:
            snapshot(driver, "", events, "final-diagnostics") if driver.session else None
        except Exception as error:
            receipt["finalDiagnosticError"] = str(error)
        try:
            driver.close()
        except Exception as error:
            receipt["sessionCloseError"] = str(error)
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
