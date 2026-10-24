/// <reference types="vite/client" />
import { expect, inject, it } from "vitest";
import { commands } from "vitest/browser";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import wasmUrl from "../../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm?url";
import type {} from "./setup.js";

async function workerUrl(): Promise<string> {
  const command = Reflect.get(commands, "sqliteE2eeWorkerUrl");
  if (typeof command !== "function") throw new Error("SQLite browser worker command is missing");
  const url: unknown = await command.call(commands);
  if (typeof url !== "string") throw new Error("SQLite browser worker URL is invalid");
  return url;
}

function chatSchema() {
  const app = s.defineApp({
    chats: s.table({ ownerId: s.uuid() }, {}),
    messages: s
      .table({ chatId: s.uuid(), text: s.string() }, { chat: s.rel("chats", "chatId") })
      .encrypted({ space: "chatId", columns: ["text"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.chats.allowRead.always();
    policy.chats.allowInsert.where({ ownerId: session.user.account });
    policy.messages.allowRead.always();
    policy.messages.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  return { app, permissions };
}

it("creates and reopens an encrypted chat through the SQLite authority and browser owner", async () => {
  const server = inject("sqliteE2eeServer");
  const { app, permissions } = chatSchema();
  const brokerWorkerUrl = await workerUrl();
  const runtimeSources = { wasmUrl, brokerWorkerUrl, wasmVersion: "sqlite-e2ee-test" };
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({ ...server, schema: app, permissions });
    const accounts = await createAccountManager({ ...server, runtimeSources });
    const account = accounts.createLocalFirst();
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      runtimeSources,
      driver: { type: "persistent" as const, dbName: `sqlite-e2ee-${crypto.randomUUID()}` },
      e2ee: { app },
    };
    db = await createDb(config);
    const chat = await db.insert(app.chats, { ownerId: account.id }).wait({ tier: "global" });
    const message = await db
      .insert(app.messages, { chatId: chat.id, text: "Encrypted in SQLite" })
      .wait({ tier: "global" });
    expect(await db.all(app.messages.where({ chatId: chat.id }), { tier: "remote" })).toEqual([
      message,
    ]);
    await db.shutdown();
    db = await createDb(config);
    expect(await db.all(app.messages.where({ chatId: chat.id }), { tier: "remote" })).toEqual([
      message,
    ]);
  } finally {
    await db?.shutdown();
  }
});

// Each account has its own persistent browser owner. Alice shares with Bob;
// Mallory can see public metadata but must never receive plaintext.
it("shares a chat between browser owners, streams replies and keeps Mallory locked out", async () => {
  const server = inject("sqliteE2eeServer");
  const { app, permissions } = chatSchema();
  const runtimeSources = {
    wasmUrl,
    brokerWorkerUrl: await workerUrl(),
    wasmVersion: "sqlite-e2ee-test",
  };
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let stop: (() => void) | undefined;
  try {
    await deploy({ ...server, schema: app, permissions });
    const accounts = await createAccountManager({ ...server, runtimeSources });
    const alice = accounts.createLocalFirst();
    const bob = accounts.createLocalFirst();
    const mallory = accounts.createLocalFirst();
    const config = (account: typeof alice) => ({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      runtimeSources,
      driver: { type: "persistent" as const, dbName: `sqlite-e2ee-${crypto.randomUUID()}` },
      e2ee: { app },
    });
    const aliceConfig = config(alice);
    const bobConfig = config(bob);
    const malloryConfig = config(mallory);
    for (const settings of [aliceConfig, bobConfig, malloryConfig]) {
      const db = await createDb(settings);
      clients.push(db);
      await db.e2ee.devices.list();
    }
    const [writer, reader, intruder] = clients as [
      (typeof clients)[number],
      (typeof clients)[number],
      (typeof clients)[number],
    ];
    const chat = await writer.insert(app.chats, { ownerId: alice.id }).wait({ tier: "global" });
    const first = await writer
      .insert(app.messages, { chatId: chat.id, text: "Hello Bob" })
      .wait({ tier: "global" });
    const query = app.messages.where({ chatId: chat.id });
    await expect(reader.all(query, { tier: "remote" })).rejects.toMatchObject({
      code: "key-not-shared",
    });
    await writer.e2ee.spaces.grant(app.chats, chat.id, bob.id).wait();
    expect(await reader.all(query, { tier: "remote" })).toEqual([first]);
    await expect(intruder.all(query, { tier: "remote" })).rejects.toMatchObject({
      code: "key-not-shared",
    });

    let snapshot: unknown;
    stop = writer.subscribe(
      query,
      {
        onUpdate: (rows) => {
          snapshot = rows;
        },
        onError: (error) => {
          snapshot = error;
        },
      },
      { tier: "remote" },
    );
    await expect.poll(() => snapshot, { timeout: 15_000 }).toEqual([first]);
    const reply = await reader
      .insert(app.messages, { chatId: chat.id, text: "Hello Alice" })
      .wait({ tier: "global" });
    await expect
      .poll(() => snapshot, { timeout: 15_000 })
      .toEqual(expect.arrayContaining([first, reply]));
    expect(snapshot).toHaveLength(2);
    stop();
    stop = undefined;
    await reader.shutdown();
    clients[1] = await createDb(bobConfig);
    const reopened = await clients[1]!.all(query, { tier: "remote" });
    expect(reopened).toEqual(expect.arrayContaining([first, reply]));
    expect(reopened).toHaveLength(2);
    await expect(intruder.all(query, { tier: "remote" })).rejects.toMatchObject({
      code: "key-not-shared",
    });
  } finally {
    stop?.();
    for (const db of clients) await db.shutdown();
  }
});
