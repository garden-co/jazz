import { expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { schema } from "../../src/schema-namespace.js";
import { createAccountManager, createJazzClient } from "../../src/react-native/index.js";
import { createNativeAccountTestSession } from "../../src/_dev/native-account-session.js";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

it("opens a borrowed local-first account foreground and subscribes without browser crypto", async () => {
  const directory = await mkdtemp(join(tmpdir(), "rn-local-first-borrowed-"));
  const host = createPlatformHost(directory);
  installPlatformHost(host);
  let client: Awaited<ReturnType<typeof createJazzClient>> | undefined;
  let lease: Awaited<ReturnType<typeof createNativeAccountTestSession>> | undefined;
  let stop = () => {};
  try {
    vi.stubGlobal("crypto", undefined);
    let stored: string | null = null;
    const accounts = await createAccountManager({
      appId: "rn-local-first-borrowed",
      serverUrl: "https://core.example",
      store: {
        async read() {
          return stored;
        },
        async update(transform) {
          stored = transform(stored);
        },
      },
    });
    const account = accounts.createLocalFirst();
    const config = { appId: "rn-local-first-borrowed", account };
    const app = schema.defineApp({ todos: schema.table({ title: schema.string() }) });
    lease = await createNativeAccountTestSession(config, serializeSchemaSource(app.wasmSchema));
    client = await createJazzClient({ ...config, nativeRelay: { capability: lease.capability } });
    expect(client.session).toMatchObject({
      authMode: "local-first",
      user: { account: account.id, identity: account.identity },
    });
    const snapshots: unknown[][] = [];
    stop = client.db.subscribe(app.todos, (rows) => snapshots.push(rows));
    await expect.poll(() => snapshots.at(-1)).toEqual([]);
    const created = await client.db
      .insert(app.todos, { title: "native account write" })
      .wait({ tier: "local" });
    await expect
      .poll(() => snapshots.at(-1))
      .toMatchObject([{ id: created.id, title: "native account write" }]);
    await expect(client.db.all(app.todos)).resolves.toMatchObject([
      { id: created.id, title: "native account write" },
    ]);
    const tx = client.db.beginTransaction();
    tx.update(app.todos, created.id, { title: "native transaction" });
    await expect(tx.all(app.todos)).resolves.toMatchObject([{ title: "native transaction" }]);
    await tx.rollback();
    await expect(client.db.all(app.todos)).resolves.toMatchObject([
      { title: "native account write" },
    ]);
    expect(accounts.getLoggedIn()).toBe(account);
  } finally {
    stop();
    await client?.shutdown();
    lease?.close();
    host.close();
    vi.unstubAllGlobals();
    await rm(directory, { recursive: true, force: true });
  }
});
