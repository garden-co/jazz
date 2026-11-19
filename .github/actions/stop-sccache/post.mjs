import { spawnSync } from "node:child_process";

// The daemon was started with the SCCACHE_* variables setup-build exported,
// which post steps inherit, so this reaches that daemon's socket.
const result = spawnSync("sccache", ["--stop-server"], { stdio: "inherit" });
if (result.error || result.status !== 0)
  console.log(
    `::warning::sccache --stop-server failed (${result.error?.message ?? `exit ${result.status}`}); the sticky disk may not commit`,
  );
