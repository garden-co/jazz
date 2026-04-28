from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Protocol


class TransportError(RuntimeError):
    def __init__(
        self,
        command: str,
        message: str,
        *,
        returncode: int | None = None,
        stdout: str | None = None,
        stderr: str | None = None,
        timed_out: bool = False,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out


class Transport(Protocol):
    def record_context_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def list_context_digests(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        ...


class SubprocessAgentInfraTransport:
    """Stable Python boundary to the agent-infra CLI.

    The Python package deliberately talks to the built CLI instead of embedding
    TypeScript source strings. That keeps Hermes and other Python agents on the
    same app-level store API used by TS callers.
    """

    def __init__(
        self,
        *,
        backend_root: str | Path | None = None,
        data_path: str | Path | None = None,
        node_bin: str | None = None,
        timeout_seconds: float = 3.0,
    ) -> None:
        self.backend_root = self._resolve_backend_root(backend_root)
        self.data_path = Path(
            data_path
            or os.environ.get("JAZZ2_AGENT_INFRA_DATA_PATH")
            or Path.home() / ".jazz2" / "agent-infra.db"
        ).expanduser()
        self.node_bin = node_bin or os.environ.get("NODE_BIN") or "node"
        self.timeout_seconds = timeout_seconds

    def record_context_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self._run_json("record-context-digest", payload)
        if not isinstance(result, dict):
            raise TransportError("record-context-digest", "CLI returned a non-object digest")
        return result

    def list_context_digests(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        args: list[str] = []
        flag_map = {
            "targetSession": "--target-session",
            "targetConversation": "--target-conversation",
            "targetConversationHash": "--target-conversation-hash",
            "targetTurnOrdinal": "--target-turn-ordinal",
            "sourceSession": "--source-session",
            "kind": "--kind",
            "limit": "--limit",
        }
        for key, flag in flag_map.items():
            value = query.get(key)
            if value is not None and value != "":
                args.extend([flag, str(value)])
        if query.get("includeExpired"):
            args.append("--include-expired")
        result = self._run_json("list-context-digests", None, args)
        if not isinstance(result, list):
            raise TransportError("list-context-digests", "CLI returned a non-list result")
        return [row for row in result if isinstance(row, dict)]

    def _run_json(
        self,
        command: str,
        payload: dict[str, Any] | None = None,
        args: list[str] | None = None,
    ) -> Any:
        cli = self.backend_root / "dist" / "src" / "cli.js"
        if not cli.exists():
            raise TransportError(
                command,
                f"agent-infra CLI is not built at {cli}; run pnpm --dir {self.backend_root} build",
            )
        full_command = [
            self.node_bin,
            str(cli),
            command,
            "--data-path",
            str(self.data_path),
            *(args or []),
        ]
        try:
            proc = subprocess.run(
                full_command,
                input=json.dumps(payload) if payload is not None else None,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
                cwd=str(self.backend_root),
            )
        except subprocess.TimeoutExpired as exc:
            raise TransportError(
                command,
                f"agent-infra CLI timed out after {self.timeout_seconds:.2f}s",
                stdout=exc.stdout,
                stderr=exc.stderr,
                timed_out=True,
            ) from exc
        if proc.returncode != 0:
            raise TransportError(
                command,
                (proc.stderr or proc.stdout or f"CLI exited {proc.returncode}").strip(),
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
            )
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise TransportError(
                command,
                "agent-infra CLI returned invalid JSON",
                stdout=proc.stdout,
                stderr=proc.stderr,
            ) from exc

    @staticmethod
    def _resolve_backend_root(value: str | Path | None) -> Path:
        if value:
            return Path(value).expanduser().resolve()
        env_value = os.environ.get("JAZZ2_AGENT_INFRA_BACKEND")
        if env_value:
            return Path(env_value).expanduser().resolve()
        default = Path.home() / "repos" / "garden-co" / "jazz2" / "examples" / "agent-infra-backend"
        if default.exists():
            return default.resolve()
        current = Path(__file__).resolve()
        for parent in current.parents:
            candidate = parent / "examples" / "agent-infra-backend"
            if candidate.exists():
                return candidate.resolve()
        found = shutil.which("agent-infra-backend")
        if found:
            return Path(found).resolve().parent
        return default.resolve()
