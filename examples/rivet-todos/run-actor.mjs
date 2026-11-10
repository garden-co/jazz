import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

for (const key of ["RIVET_ENDPOINT", "RIVET_TOKEN", "RIVET_POOL", "JAZZ_CORE_SECRET"]) {
  if (!process.env[key]) throw new Error(`${key} is required in .env.local`);
}
const child = spawn("cargo", ["run", "--locked", "--bin", "jazz-rivet"], {
  cwd: fileURLToPath(new URL("../../crates/jazz-rivet/", import.meta.url)),
  env: {
    ...process.env,
    LIBSQLITE3_FLAGS: "SQLITE_ENABLE_BATCH_ATOMIC_WRITE SQLITE_DIRECT_OVERFLOW_READ=0",
    RUST_LOG: process.env.RUST_LOG ?? "info",
  },
  stdio: "inherit",
});
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => child.kill(signal));
child.once("error", (error) => {
  console.error(error.message);
  process.exitCode = 1;
});
child.once("exit", (code, signal) => {
  process.exitCode = code ?? (signal ? 1 : 0);
});
