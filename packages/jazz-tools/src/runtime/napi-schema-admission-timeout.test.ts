import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NapiDb } from "jazz-napi";
import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";

// A connected carrier that never delivers a catalogue must preserve the native
// read deadline, even though no application query can be attached yet (#2999).
it("times out pending schema admission on a silent native upstream and closes", async () => {
  const before = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
  const after = s.defineApp({
    notes: s.table({ text: s.string() }, {}),
    controls: s.table({ value: s.string() }, {}),
  });
  const path = await mkdtemp(join(tmpdir(), "jazz-schema-admission-timeout-"));
  const config = openConfig(
    new Uint8Array(16).fill(29),
    testAuthorBytes("schema-admission-timeout"),
    1,
    true,
  );
  let db: NapiDb | undefined;
  try {
    db = NapiDb.openPersistentAsBackend(path, encodeSchema(before.wasmSchema), config);
    await db.close();
    db = NapiDb.openPersistentAsBackend(path, encodeSchema(after.wasmSchema), config);
    const silent = db.connectUpstream();
    db.tick();
    silent.recvWireFrames(); // Deliberately send no response to the connected carrier.
    const started = performance.now();
    const pending = db.all(queryFromTable("notes"), { tier: "global" });
    if (pending instanceof Uint8Array) throw new Error("unadmitted schema returned rows");
    expect(pending.poll()).toBeNull();
    let rejection: unknown;
    while (performance.now() - started < 20_000) {
      await new Promise((resolve) => setTimeout(resolve, 5));
      db.tick();
      try {
        if (pending.poll() !== null) throw new Error("unadmitted schema returned rows");
      } catch (error) {
        rejection = error;
        break;
      }
    }
    expect(rejection).toBeInstanceOf(Error);
    expect(String(rejection)).toContain("NotObserved: Timed out waiting for query coverage");
    expect(performance.now() - started).toBeGreaterThanOrEqual(15_000);
  } finally {
    if (db) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          db.close(),
          new Promise<never>((_, reject) => {
            timer = setTimeout(
              () => reject(new Error("close stuck after admission timeout")),
              2_000,
            );
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    }
    await rm(path, { recursive: true, force: true });
  }
}, 25_000);
