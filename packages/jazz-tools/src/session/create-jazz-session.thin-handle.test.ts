import { afterEach, describe, expect, it, vi } from "vitest";
import { memoryPrivateStore } from "../encryption/index.js";
import type { Db } from "../runtime/db.js";
import { DbReplacedError } from "../runtime/db-replaced-error.js";
import { EncryptionNeedsLocalFirstError } from "../runtime/encryption-needs-local-first.js";
import { isThinDb, type LocalFirstLoader } from "../runtime/local-first-upgrade.js";
import { cleanUpTestApp, memoryTestAccountStore as memoryStore } from "../testing/index.js";
import { openBrowserSessionCore, type BrowserJazzSession } from "./create-jazz-session.js";
import {
  gatedLoader,
  serveThinTestApp,
  thinHandleApp as app,
  thinHandlePermissions as permissions,
} from "../../tests/fixtures/thin-session.js";

// A handle taken from `db.encryption` while the Db is thin keeps working after
// the local-first upgrade, and reports the Db's own error after sign-out.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  await cleanUpTestApp(cleanups);
  vi.restoreAllMocks();
});

const serve = () => serveThinTestApp(app, permissions, cleanups);
type Host = Awaited<ReturnType<typeof serve>>;

/** A thin session of an account the app signs in itself. */
async function thinSession(host: Host, client: LocalFirstLoader): Promise<BrowserJazzSession> {
  const session = await openBrowserSessionCore({
    scope: host.jazz.appId,
    serverUrl: host.serverUrl,
    store: memoryStore(),
    driver: { type: "memory" },
    bearer: () => "token-alice",
    encryption: { app, store: memoryPrivateStore() },
    client,
  } as never);
  cleanups.push(() => session.close());
  return session;
}

const db = (session: BrowserJazzSession): Db => session.getSnapshot().client!.db;

function sync(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

describe("a db.encryption handle taken while thin", () => {
  it("keeps working after the upgrade: recovery.create() runs and stop() detaches", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await thinSession(host, gate.loader);
    const kept = db(session);
    expect(isThinDb(kept)).toBe(true);
    const handle = kept.encryption;
    const refused = sync(() => handle.recovery.create());
    expect(refused).toBeInstanceOf(EncryptionNeedsLocalFirstError);
    expect((refused as EncryptionNeedsLocalFirstError).reason).toBe("not-loaded");
    handle.onReport(() => {});

    gate.release();
    await handle.ready({ timeoutMs: 20_000 });
    expect(isThinDb(kept)).toBe(false);

    const real = kept.encryption as unknown as { listeners: Set<unknown> };
    expect(real.listeners.size).toBe(1);
    expect(handle.recovery.create().split(" ")).toHaveLength(24);
    handle.stop();
    expect(real.listeners.size).toBe(0);
  }, 90_000);

  it("reports the Db's replaced error after sign-out, not not-loaded", async () => {
    const host = await serve();
    const gate = gatedLoader();
    const session = await thinSession(host, gate.loader);
    const handle = db(session).encryption;
    await session.logout();

    expect(sync(() => handle.recovery.create())).toBeInstanceOf(DbReplacedError);
    await expect(handle.deviceId()).rejects.toBeInstanceOf(DbReplacedError);
  }, 60_000);
});
