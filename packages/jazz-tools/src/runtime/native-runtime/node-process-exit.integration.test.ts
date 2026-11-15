import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";

// A Node process with a connected client and a local server must exit on its
// own once both stop: nothing the runtime registered with Node (timers, napi
// callbacks) may keep the event loop alive afterwards.

// The built package, as a Node app loads it.
const builtTesting = new URL("../../../dist/testing/index.js", import.meta.url).href;

const EXIT_TIMEOUT_MS = 10_000;

function runToExit(
  script: string,
): Promise<{ code: number | null; timedOut: boolean; output: string }> {
  const child = spawn(process.execPath, ["--input-type=module", "--eval", script], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout?.on("data", (chunk) => (output += chunk));
  child.stderr?.on("data", (chunk) => (output += chunk));
  return new Promise((resolve) => {
    let stoppedAt: number | undefined;
    let timer: NodeJS.Timeout | undefined;
    const watchStopped = () => {
      if (stoppedAt !== undefined || !output.includes("stopped\n")) return;
      stoppedAt = Date.now();
      timer = setTimeout(() => {
        child.kill("SIGKILL");
      }, EXIT_TIMEOUT_MS);
    };
    child.stdout?.on("data", watchStopped);
    // Bound the whole run too, so a hang before "stopped" fails rather than stalls.
    const overall = setTimeout(() => child.kill("SIGKILL"), 60_000);
    child.on("exit", (code, signal) => {
      clearTimeout(overall);
      if (timer) clearTimeout(timer);
      resolve({ code, timedOut: signal === "SIGKILL", output });
    });
  });
}

describe("a Node process with a client and a server", () => {
  it("exits on its own after both stop", async () => {
    const script = [
      `import { randomUUID } from "node:crypto";`,
      `import { deploy, startLocalJazzServer, startTestJazz, testApp } from ${JSON.stringify(builtTesting)};`,
      `const app = testApp({ tables: { notes: { columns: { label: "string" } } } });`,
      `const permissions = { format: 1, tables: {} };`,
      `const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });`,
      `const session = await startTestJazz({ app, permissions, core: server });`,
      `await deploy({ serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret, app, permissions });`,
      `await session.jazz.db.insert(app.notes, { label: "synced" }).wait({ tier: "global" });`,
      `await session.stop();`,
      `await server.stop();`,
      `process.stdout.write("stopped\\n");`,
    ].join("\n");

    const result = await runToExit(script);

    expect(result.output, result.output).toContain("stopped\n");
    expect(
      result.timedOut,
      `still running ${EXIT_TIMEOUT_MS}ms after stop:\n${result.output}`,
    ).toBe(false);
    expect(result.code, result.output).toBe(0);
  }, 90_000);
});
