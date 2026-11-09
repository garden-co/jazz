// ServerCore hosting: replica sync/rejection, independent objects, socket
// cleanup, and reconstruction with only SQLite retained.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHarness } from "./runtime.mjs";

const executable = fileURLToPath(
  new URL("../../../../target/debug/doc-shelf-do-ceiling-ws", import.meta.url),
);
const persist = await mkdtemp(join(tmpdir(), "jazz-server-core-"));
async function control(url, request = { op: "stats" }) {
  const ws = new WebSocket(`${url}/control`);
  try {
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("control timeout"));
        ws.close();
      }, 10_000);
      ws.addEventListener("open", () => ws.send(JSON.stringify(request)), { once: true });
      ws.addEventListener(
        "message",
        (event) => {
          clearTimeout(timer);
          resolve(JSON.parse(event.data));
        },
        { once: true },
      );
      ws.addEventListener(
        "error",
        () => {
          clearTimeout(timer);
          reject(new Error("control socket failed"));
        },
        { once: true },
      );
    });
  } finally {
    ws.close();
  }
}
try {
  for (const verify of [false, true]) {
    const mf = createHarness({ port: 0, durableObjectsPersist: persist });
    try {
      const url = new URL(await mf.ready);
      url.protocol = "ws:";
      for (const object of ["first", "second"]) {
        const base = `${url.href.replace(/\/$/, "")}/${object}`;
        if (!verify) {
          const empty = await control(base, { op: "reopen", config: "" });
          assert.equal(empty.ok, false);
          assert.match(empty.error, /no admitted catalogue schema/);
        }
        const env = { ...process.env, VALUE_TEST: "1" };
        delete env.NATIVE;
        delete env.SCOPE_TEST;
        delete env.VERIFY_ONLY;
        delete env.REOPEN_CATALOGUE;
        if (verify) {
          env.VERIFY_ONLY = "1";
          env.REOPEN_CATALOGUE = "1";
        }
        await new Promise((resolve, reject) => {
          const child = spawn(executable, [base], { env, stdio: "inherit", timeout: 120_000 });
          child.once("error", reject);
          child.once("exit", (code, signal) =>
            code === 0 ? resolve() : reject(new Error(`client failed: ${signal ?? code}`)),
          );
        });
        let result;
        for (let attempt = 0; attempt < 20; attempt++) {
          result = await control(base);
          assert.equal(result.ok, true);
          assert.equal(result.runtime, "ServerCore");
          assert.equal(result.startup, verify ? "catalogue" : "published");
          assert.deepEqual(result.errors, []);
          if (result.sessions === 0) break;
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        assert.equal(result.sessions, 0, "disconnected sockets are released");
      }
    } finally {
      await mf.dispose();
    }
  }
  console.log(
    "ServerCore sync, rollback, object isolation, disconnect, and restart checks passed.",
  );
} finally {
  await rm(persist, { recursive: true, force: true });
}
