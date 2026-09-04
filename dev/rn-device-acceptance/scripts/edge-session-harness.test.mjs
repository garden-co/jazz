import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  assertCoreObservation,
  boundedHarnessOutput,
  startLocalEdgeSessionHarness,
} from "./edge-session-harness.mjs";

const nonce = "12345678-1234-4234-9234-123456789abc";
const valid = {
  source: "core",
  runNonce: nonce,
  title: `high-level-foreground-row:${nonce}`,
  rowId: "12345678-1234-4234-9234-123456789def",
};

test("Core observation rejects missing, wrong-source, stale-run, wrong-title and malformed-row evidence", () => {
  assert.equal(assertCoreObservation(valid, nonce), valid);
  for (const planted of [
    undefined,
    {},
    { ...valid, source: "edge" },
    { ...valid, runNonce: "old-run" },
    { ...valid, title: "old-row" },
    { ...valid, rowId: "" },
  ]) {
    assert.throws(
      () => assertCoreObservation(planted, nonce),
      /invalid run-bound Core observation/,
    );
  }
});

test("shared harness requires a child-produced run-bound observation and handles split readiness", async () => {
  const directory = mkdtempSync(join(tmpdir(), "jazz-rn-harness-contract-"));
  const oldPath = process.env.PATH;
  try {
    for (const planted of [valid, { ...valid, source: "edge" }, null]) {
      writeFileSync(
        join(directory, "cargo"),
        `#!${process.execPath}
if (process.argv[2] === "build") process.exit(0);
const line = 'JAZZ_RN_EDGE_SESSION ' + JSON.stringify({edge_port:12345,bearer_a:'ephemeral-a',bearer_b:'ephemeral-b'}) + '\\n';
process.stdout.write(line.slice(0, 25));
setTimeout(() => {
  process.stdout.write(line.slice(25));
  const observation = ${JSON.stringify(planted)};
  if (observation) process.stdout.write('JAZZ_RN_CORE_OBSERVATION ' + JSON.stringify(observation) + '\\n');
}, 20);
setInterval(() => {}, 1000);
`,
        { mode: 0o755 },
      );
      process.env.PATH = `${directory}:${oldPath}`;
      const harness = await startLocalEdgeSessionHarness({
        device: "contract-device",
        runNonce: nonce,
        host: "127.0.0.1",
      });
      try {
        assert.equal(harness.endpoint, "http://127.0.0.1:12345");
        if (planted === valid) assert.deepEqual(await harness.waitForCoreObservation(150), valid);
        else
          await assert.rejects(
            harness.waitForCoreObservation(150),
            /invalid run-bound Core observation|missing run-bound Core observation/,
          );
      } finally {
        harness.child.kill("SIGTERM");
      }
    }
  } finally {
    process.env.PATH = oldPath;
    rmSync(directory, { recursive: true, force: true });
  }
});

test("ephemeral session lines and JWTs stay out of failure diagnostics", () => {
  const secret = "eyJabc.eyJdef.signature";
  const safe = boundedHarnessOutput(`JAZZ_RN_EDGE_SESSION {"bearer":"${secret}"}\nerror ${secret}`);
  assert.ok(!safe.includes(secret));
  assert.ok(safe.includes("[redacted]"));
});

test("both installed drivers gate termination on Core evidence and clean up the harness", () => {
  for (const name of ["android", "ios"]) {
    const source = readFileSync(new URL(`./run-${name}.mjs`, import.meta.url), "utf8");
    const seed = source.indexOf('await launchAndAssert("seed")');
    const observed = source.indexOf("await localSession.waitForCoreObservation()");
    const verify = source.indexOf('await launchAndAssert("verify")');
    assert.ok(seed >= 0 && observed > seed && verify > observed);
    assert.match(source, /finally\s*\{\s*localSession\.child\.kill\("SIGTERM"\)/);
  }
});

test("native acknowledgement checks every identity field and stays pending until Core observation", async () => {
  const { startCoreObservationControl } = await import("./core-observation-control.mjs");
  let release;
  let calls = 0;
  const observed = new Promise((resolve) => {
    release = resolve;
  });
  const expected = {
    platform: "ios",
    deviceIdentifier: "simulator",
    buildFingerprint: "a".repeat(64),
    runNonce: nonce,
  };
  const control = await startCoreObservationControl({
    session: {
      async waitForCoreObservation() {
        calls++;
        await observed;
        return valid;
      },
    },
    expected,
    host: "127.0.0.1",
  });
  const post = (identity) =>
    fetch(control.endpoint, { method: "POST", body: JSON.stringify(identity) });
  try {
    for (const key of Object.keys(expected))
      assert.equal((await post({ ...expected, [key]: "foreign" })).status, 403);
    assert.equal(calls, 0, "foreign native identity must not enter the Core observation gate");
    let completed = false;
    const pending = post(expected).then((response) => {
      completed = true;
      return response;
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(completed, false, "no acknowledgement can precede a Core observation");
    release();
    assert.equal((await pending).status, 204);
    assert.equal(calls, 1);
  } finally {
    release();
    await control.close();
  }
});

test("missing Core observation cannot release the native foreground", async () => {
  const { startCoreObservationControl } = await import("./core-observation-control.mjs");
  const expected = {
    platform: "android",
    deviceIdentifier: "emulator",
    buildFingerprint: "b".repeat(64),
    runNonce: nonce,
  };
  const control = await startCoreObservationControl({
    session: {
      async waitForCoreObservation() {
        throw new Error("planted missing Core write");
      },
    },
    expected,
    host: "127.0.0.1",
  });
  try {
    assert.equal(
      (await fetch(control.endpoint, { method: "POST", body: JSON.stringify(expected) })).status,
      503,
    );
  } finally {
    await control.close();
  }
});

function assertCoreObserverContract(source) {
  const context = /let observer = connect\(AppContext \{([\s\S]*?)\}\)/.exec(source)?.[1];
  assert.ok(context, "observer must use an explicit isolated client context");
  assert.match(context, /server_url: core\.base_url\(\)/, "observer must connect directly to Core");
  assert.match(context, /storage: ClientStorage::Memory/);
  assert.doesNotMatch(
    source,
    /\.insert\(|\.insert_with_id\(|\.update\(|\.upsert\(/,
    "observer harness cannot manufacture the device write",
  );
  assert.match(source, /wait_for_query\(\s*&observer,/);
  assert.match(source, /values\.contains\(&Value::Text\(title\.clone\(\)\)\)/);
}

test("observer source stays read-only and Core-connected; planted Edge reader and writer fail", () => {
  const source = readFileSync(
    new URL(
      "../../../crates/jazz-native-relay/examples/rn_edge_session_harness.rs",
      import.meta.url,
    ),
    "utf8",
  );
  assertCoreObserverContract(source);
  assert.throws(
    () =>
      assertCoreObserverContract(
        source.replace("server_url: core.base_url()", "server_url: edge.base_url()"),
      ),
    /directly to Core/,
  );
  assert.throws(
    () => assertCoreObserverContract(`${source}\n observer.insert("todos", fake);`),
    /cannot manufacture/,
  );
});
