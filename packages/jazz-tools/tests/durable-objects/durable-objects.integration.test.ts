import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { it } from "vitest";

const execute = promisify(execFile);
// Separate processes retain the tests' workerd restart/fault boundaries and
// ensure one suite's global transport interception cannot leak into another.
for (const [name, args, packaged] of [
  ["create-jazz", [], true],
  ["worker-backend", [], true],
  ["accounts", [], true],
  ["thin-parts", [], true],
  ["lifecycle", [], true],
  ["crash", [], true],
  ["catalogue", [], false],
  ["scopes", ["--catalogue"], false],
  ["values", ["--sync"], false],
  ["server-core", [], false],
] as const) {
  // Crash needs a replica writer: createJazz's thin client cannot begin transactions.
  // Scopes still expects transactions to begin; managed storage now rejects them early.
  it.skipIf(name === "crash" || name === "scopes")(`DO ${name}`, async () => {
    try {
      await execute(
        process.execPath,
        [fileURLToPath(new URL(`./test-${name}.mjs`, import.meta.url)), ...args],
        {
          env: { ...process.env, JAZZ_DO_PACKAGED: packaged ? "1" : "0" },
          timeout: 170_000,
          maxBuffer: 2 * 1024 * 1024,
        },
      );
    } catch (error) {
      const failure = error as Error & { stdout?: string; stderr?: string };
      throw new Error(`${failure.message}\n${failure.stdout ?? ""}\n${failure.stderr ?? ""}`, {
        cause: error,
      });
    }
  });
}
