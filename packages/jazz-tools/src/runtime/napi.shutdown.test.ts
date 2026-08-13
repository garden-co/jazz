import { execFile } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const here = dirname(fileURLToPath(import.meta.url));
const contextFixture = join(here, "__fixtures__/napi-shutdown-context.ts");
const callbacksFixture = join(here, "__fixtures__/napi-shutdown-callbacks.ts");
const execFileAsync = promisify(execFile);

function runFixture(fixturePath: string) {
  return execFileAsync(process.execPath, ["--import", import.meta.resolve("tsx"), fixturePath], {
    encoding: "utf8",
    timeout: 5_000,
  });
}

describe("NAPI shutdown", () => {
  it("allows the Node process to exit after JazzContext.shutdown()", async () => {
    const result = await runFixture(contextFixture);

    expect(result.stderr).toBe("");
    expect(result.stdout).toBe("shutdown complete\n");
  }, 10_000);

  it("releases all registered host callbacks when the runtime closes", async () => {
    const result = await runFixture(callbacksFixture);

    expect(result.stderr).toBe("");
    expect(result.stdout).toBe("runtime closed\n");
  }, 10_000);
});
