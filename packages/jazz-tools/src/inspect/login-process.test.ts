import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it } from "vitest";
import { decodeGrant, parseInspectorLink } from "./protocol.js";

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

const TOKEN = "jzi1_test-token";

/** A backend whose Inspector API only knows `whoami`, as the CLI asks once it has a token. */
async function backend(): Promise<string> {
  const server: Server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      const ok =
        req.url === "/jazz/inspect/api" &&
        req.headers.authorization === `Bearer ${TOKEN}` &&
        JSON.parse(body).op === "whoami";
      res.writeHead(ok ? 200 : 401, { "content-type": "application/json" });
      res.end(
        JSON.stringify(
          ok
            ? {
                by: "ops@example.com",
                write: false,
                expiresAt: Math.floor(Date.now() / 1000) + 600,
              }
            : { error: "unauthorized", message: "no" },
        ),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise((resolve) => server.close(resolve)));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}/jazz`;
}

// `jazz-tools inspect login` must still be running when the browser comes
// back from the approval page. It used to exit as soon as it printed the
// URL (nothing held the process open), so the browser's redirect to the
// loopback callback was refused and no token was stored.
it("`inspect login` waits in its own process for the approval, then stores the token", async () => {
  const app = await backend();
  const home = await mkdtemp(join(tmpdir(), "jazz-inspect-login-"));
  cleanups.push(() => rm(home, { recursive: true, force: true }));
  const cli = fileURLToPath(new URL("./cli.ts", import.meta.url));
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      "--input-type=module",
      "-e",
      `const { runInspectCommand } = await import(${JSON.stringify(cli)});
       process.exitCode = await runInspectCommand(process.argv.slice(1), {
         stdout: (text) => process.stdout.write(text),
         stderr: (text) => process.stderr.write(text),
         env: process.env,
       });`,
      "login",
      "--app",
      app,
      "--no-browser",
    ],
    {
      env: { ...process.env, JAZZ_CONFIG_DIR: home, JAZZ_INSPECT_TOKEN: "", JAZZ_APP_URL: "" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  cleanups.push(async () => child.kill());
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));
  const exited = new Promise<number | null>((resolve) => child.on("exit", resolve));

  const deadline = Date.now() + 20_000;
  while (!stdout.includes("\n") && Date.now() < deadline) {
    if (child.exitCode !== null) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  const step = JSON.parse(stdout.split("\n")[0]!) as { step: string; url: string };
  expect(step.step, stderr).toBe("approve");

  // A person takes a moment to approve; the CLI must still be listening.
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect(child.exitCode, `exited before approval: ${stderr}`).toBeNull();

  const grant = decodeGrant(parseInspectorLink(new URL(step.url).hash).grant!)!;
  const handed = await fetch(`${grant.callback}?state=${grant.state}&token=${TOKEN}`);
  expect(handed.status).toBe(200);

  expect(await exited).toBe(0);
  expect(stdout).toContain('"step":"done"');
  const stored = JSON.parse(await readFile(join(home, "inspect-tokens.json"), "utf8"));
  expect(JSON.stringify(stored)).toContain(TOKEN);
}, 40_000);
