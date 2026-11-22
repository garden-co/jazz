import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import permissions from "../../tests/fixtures/encrypted/jazz.permissions.json";
import { testApp } from "../testing/index.js";
import { createJazz, memory, type CreateJazzOptions } from "../backend/index.js";
import { createDb as createLocalFirstDb } from "../runtime/default-create-db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import type { AccountStore } from "../accounts/persistence.js";
import { openBrowserSessionCore as createJazzClient } from "../session/create-jazz-session.js";
import { EncryptionNotDeclaredError, memoryPrivateStore, type PrivateStore } from "./index.js";

// End to end on the browser-style client (the WebAssembly runtime, which Node,
// Deno, Bun and CLIs also use with a bearer) against a real server: the app
// signs its users in, so their accounts are the app's, not device accounts, and
// recover with a phrase rather than by their own secret. Each client opens its
// own runtime over a private store; a store shared by two clients is two tabs of
// one device.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

/** The app's own sign-in: `Authorization: Bearer token-<user>`. */
function authenticate(request: Request) {
  const match = /^Bearer token-(\w+)$/.exec(request.headers.get("authorization") ?? "");
  return match ? { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
}

const plainApp = testApp({ tables: { notes: { columns: { text: "string" } } } });

async function serve(
  served: Pick<CreateJazzOptions, "app" | "permissions"> = { app, permissions },
) {
  const jazz = await createJazz({ ...served, store: memory(), authenticate }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return { jazz, serverUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}/jazz` };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

/** Waits until `read` returns something. */
async function until<T>(read: () => Promise<T | undefined>, what: string, ms = 30_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    let failure: unknown;
    const value = await read().catch((error) => {
      failure = error;
      return undefined;
    });
    if (value !== undefined) return value;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}: ${String(failure)}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/** A client of `user` with its own private store (or a shared one: a second tab). */
async function client(
  host: { serverUrl: string },
  user: string,
  options: { privateStore?: PrivateStore; accounts?: AccountStore; plain?: boolean } = {},
) {
  const session = await createJazzClient({
    serverUrl: host.serverUrl,
    store: options.accounts ?? memoryStore(),
    driver: { type: "memory" },
    bearer: () => `token-${user}`,
    async client() {
      return { createClient: createLocalFirstDb };
    },
    ...(options.plain
      ? {}
      : { encryption: { app, store: options.privateStore ?? memoryPrivateStore() } }),
  } as never);
  cleanups.push(() => session.close());
  // The local-first client replaces the thin one once it has loaded.
  await until(
    async () => (isThinDb(session.getSnapshot().client!.db) ? undefined : true),
    "the local-first client",
  );
  const live = session.getSnapshot().client!;
  return { session, db: live.db, account: live.session!.user! };
}

type Client = Awaited<ReturnType<typeof client>>;

async function makeProject(owner: Client, name: string) {
  const id = crypto.randomUUID();
  const self = await owner.db.encryption.members.row({ table: "projects", id }, owner.account);
  const written = await owner.db.transaction((tx) => {
    tx.insert(app.projects, { name, ownerId: owner.account }, { id });
    tx.insert(app.__e2ee_members, self);
  });
  await written.wait({ tier: "global" });
  return id;
}

/** The first device of an account, active, with a project and one encrypted task. */
async function firstDevice(host: { serverUrl: string }, user: string) {
  const first = await client(host, user);
  expect(await first.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
  const project = await makeProject(first, "plans");
  await first.db.insert(app.tasks, { projectId: project, title: "kept" }).wait({ tier: "global" });
  await first.db.encryption.keys.deliver();
  return { first, project };
}

describe("encryption on the browser-style client, end to end", () => {
  it("a second device of an app-sign-in account is approved with the challenge notice and the code, then reads", async () => {
    const host = await serve();
    const { first, project } = await firstDevice(host, "alice");
    const second = await client(host, "alice");
    expect(await second.db.encryption.ready({ timeoutMs: 30_000 })).toBe("awaiting-approval");
    // Nothing reads before approval.
    const challenged = new Promise<void>((resolve) =>
      second.db.encryption.onChallenge(() => resolve()),
    );

    const waiting = await until(async () => {
      const devices = await first.db.encryption.devices.list();
      return devices.find((device) => device.status === "waiting");
    }, "the new device to be listed as waiting");
    const approval = await first.db.encryption.devices.challenge(waiting.id);
    await challenged;
    const code = await second.db.encryption.devices.answer();
    expect(code).toMatch(/^\d{6}$/);
    const shown = await until(() => approval.code(), "the approving device's code");
    expect(shown).toBe(code);
    await expect(approval.approve("000000" === code ? "111111" : "000000")).rejects.toMatchObject({
      code: "approval-code-mismatch",
    });
    await approval.approve(code);
    expect(await second.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");

    const rows = await until(async () => {
      const found = await second.db.all(app.tasks.where({ projectId: project }), {
        tier: "remote",
      });
      return found.length > 0 ? found : undefined;
    }, "the approved device to read");
    expect(rows[0]!.title).toBe("kept");
    expect(
      (await first.db.encryption.devices.list()).filter((d) => d.status === "active"),
    ).toHaveLength(2);
  }, 120_000);

  it("an idle active device completes another device's removal request", async () => {
    const host = await serve();
    const first = (await client(host, "alice")).db.encryption;
    expect(await first.ready()).toBe("active");
    const phrase = first.recovery.create();
    await first.recovery.register(phrase);

    // No further calls on the first device. Its account watch drives the removal.
    const second = (await client(host, "alice")).db.encryption;
    expect(await second.ready()).toBe("awaiting-approval");
    expect(await second.recovery.restore(phrase)).toBe("active");
    const oldId = await second.deviceId();
    await second.devices.requestRemoval();

    await until(
      async () => ((await second.deviceId()) !== oldId ? true : undefined),
      "a new device ID",
    );
    await expect(second.devices.list()).rejects.toMatchObject({ code: "device-not-active" });
  }, 90_000);

  it("a new device with no other device recovers with the phrase", async () => {
    const host = await serve();
    const { first, project } = await firstDevice(host, "alice");
    const phrase = first.db.encryption.recovery.create();
    expect(phrase.split(" ")).toHaveLength(24);
    await first.db.encryption.recovery.register(phrase);
    expect((await first.db.encryption.recovery.list()).length).toBe(1);
    // Every device is gone; a new one has only the phrase.
    await first.session.close();
    const fresh = await client(host, "alice");
    expect(await fresh.db.encryption.ready({ timeoutMs: 30_000 })).toBe("awaiting-approval");
    // A different phrase does not recover.
    await expect(
      fresh.db.encryption.recovery.restore(fresh.db.encryption.recovery.create()),
    ).rejects.toThrow();
    expect(await fresh.db.encryption.recovery.restore(phrase)).toBe("active");
    const rows = await until(async () => {
      const found = await fresh.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
      return found.length > 0 ? found : undefined;
    }, "the recovered device to read");
    expect(rows[0]!.title).toBe("kept");
  }, 120_000);

  it("two tabs of one device share one identity and one state, and both read and write", async () => {
    const host = await serve();
    const privateStore = memoryPrivateStore();
    const accounts = memoryStore();
    const [one, two] = await Promise.all([
      client(host, "alice", { privateStore, accounts }),
      client(host, "alice", { privateStore, accounts }),
    ]);
    // Both verify at once: one device is made, not two.
    const [a, b] = await Promise.all([
      one.db.encryption.ready({ timeoutMs: 30_000 }),
      two.db.encryption.ready({ timeoutMs: 30_000 }),
    ]);
    expect([a, b]).toEqual(["active", "active"]);
    expect(await one.db.encryption.deviceId()).toBe(await two.db.encryption.deviceId());
    const devices = await one.db.encryption.devices.list();
    expect(devices).toHaveLength(1);

    const project = await makeProject(one, "shared");
    await one.db
      .insert(app.tasks, { projectId: project, title: "from one" })
      .wait({ tier: "global" });
    await two.db
      .insert(app.tasks, { projectId: project, title: "from two" })
      .wait({ tier: "global" });
    for (const tab of [one, two]) {
      const rows = await until(async () => {
        const found = await tab.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
        return found.length > 1 ? found : undefined;
      }, "a tab to read both tasks");
      expect(rows.map((row) => row.title).sort()).toEqual(["from one", "from two"]);
    }
    // Both tabs run the delivery pass at once: harmless.
    await Promise.all([one.db.encryption.keys.deliver(), two.db.encryption.keys.deliver()]);
  }, 120_000);

  it("the store survives a reload: the same device, no new enrolment, and its rows read", async () => {
    const host = await serve();
    const privateStore = memoryPrivateStore();
    const accounts = memoryStore();
    const first = await client(host, "alice", { privateStore, accounts });
    expect(await first.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
    const device = await first.db.encryption.deviceId();
    const project = await makeProject(first, "plans");
    await first.db
      .insert(app.tasks, { projectId: project, title: "before reload" })
      .wait({ tier: "global" });
    await first.session.close();
    const again = await client(host, "alice", { privateStore, accounts });
    expect(await again.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
    expect(await again.db.encryption.deviceId()).toBe(device);
    const rows = await until(async () => {
      const found = await again.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
      return found.length > 0 ? found : undefined;
    }, "the reloaded device to read");
    expect(rows[0]!.title).toBe("before reload");
  }, 120_000);

  it("a plain app is unaffected: no runtime, writes as before, and db.encryption says so", async () => {
    const host = await serve({
      app: plainApp,
      permissions: { format: 1, tables: { notes: { read: "true", insert: "true" } } },
    });
    const session = await createJazzClient({
      serverUrl: host.serverUrl,
      store: memoryStore(),
      driver: { type: "memory" },
      bearer: () => "token-alice",
      async client() {
        return { createClient: createLocalFirstDb };
      },
    } as never);
    cleanups.push(() => session.close());
    await until(
      async () => (isThinDb(session.getSnapshot().client!.db) ? undefined : true),
      "the local-first client",
    );
    const { db } = session.getSnapshot().client!;
    const note = await db.insert(plainApp.notes, { text: "hello" }).wait({ tier: "global" });
    expect(note.text).toBe("hello");
    expect(() => db.encryption.ready()).toThrow(EncryptionNotDeclaredError);
  }, 60_000);
});
