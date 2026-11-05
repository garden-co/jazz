/// <reference types="vite/client" />
import { expect, inject, it } from "vitest";
import { commands } from "vitest/browser";
import { chatSchema } from "./chat-schema.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import wasmUrl from "../../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm?url";
import type {} from "./setup.js";
import type { ProfileScenarioResult } from "./profiles-node.js";

async function workerUrl(): Promise<string> {
  const command = Reflect.get(commands, "sqliteE2eeWorkerUrl");
  if (typeof command !== "function") throw new Error("SQLite browser worker command is missing");
  const url: unknown = await command.call(commands);
  if (typeof url !== "string") throw new Error("SQLite browser worker URL is invalid");
  return url;
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

// A separate Playwright context has independent browser storage and workers,
// just like an independent browser profile/private window while it remains open.
it("shares and restores encrypted chat across independent private browser profiles", async () => {
  const server = inject("sqliteE2eeServer");
  const { app, permissions } = chatSchema();
  await deploy({ ...server, schema: app, permissions });
  const command = Reflect.get(commands, "sqliteE2eeProfileScenario");
  if (typeof command !== "function") throw new Error("Independent profile command is missing");
  const result: ProfileScenarioResult = await command.call(commands, {
    appId: server.appId,
    serverUrl: server.serverUrl,
    runtimeSources: {
      wasmUrl: new URL(wasmUrl, location.href).href,
      brokerWorkerUrl: new URL(await workerUrl(), location.href).href,
      wasmVersion: "sqlite-e2ee-test",
    },
  });
  expect(result.profiles).toHaveLength(3);
  expect(new Set(result.profiles.map((profile) => profile.accountId)).size).toBe(3);
  for (const profile of result.profiles) {
    expect(profile).toMatchObject({
      restored: false,
      previousLabel: null,
      secureContext: true,
      subtleCrypto: true,
    });
  }
  expect(result.bobBeforeGrant).toBe("key-not-shared");
  expect(result.bobAfterGrant).toEqual([result.first]);
  expect(result.first.text).toBe("Hello from Alice's profile");
  expect(result.reply.text).toBe("Reply from Bob's private profile");
  expect(result.aliceSubscription).toEqual(expect.arrayContaining([result.first, result.reply]));
  expect(result.aliceSubscription).toHaveLength(2);
  expect(result.bobReopened).toEqual({
    ...result.profiles[1],
    restored: true,
    previousLabel: "Bob",
  });
  expect(result.bobAfterReopen).toEqual(expect.arrayContaining([result.first, result.reply]));
  expect(result.bobAfterReopen).toHaveLength(2);
  expect(result.malloryBeforeReply).toBe("key-not-shared");
  expect(result.malloryAfterReply).toBe("key-not-shared");
}, 120_000);
