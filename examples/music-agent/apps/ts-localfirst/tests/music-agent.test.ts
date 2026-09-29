import { afterEach, describe, expect, test } from "vitest";
import { createAccountManager, createDb, type Db } from "jazz-tools";
import { definePermissions } from "jazz-tools/permissions";
import { startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import { app } from "../schema.js";
import {
  DeterministicMusicAgent,
  JazzMusicStore,
  MemoryMusicStore,
  chunks,
  type MusicStore,
} from "../src/music-agent.js";

const openDbs: Db[] = [];
let server: LocalJazzServerHandle | undefined;

afterEach(async () => {
  await Promise.all(openDbs.splice(0).map((db) => db.shutdown()));
  await server?.stop();
  server = undefined;
});

/** A real local-first account on an in-memory Jazz runtime. */
async function openDb(appId: string, serverUrl?: string): Promise<Db> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl: serverUrl ?? "http://127.0.0.1:1",
    store: {
      read: async () => stored,
      update: async (transform) => {
        stored = transform(stored);
      },
    },
  });
  const db = await createDb({
    appId,
    ...(serverUrl ? { serverUrl } : {}),
    account: accounts.createLocalFirst(),
    driver: { type: "memory" },
  });
  openDbs.push(db);
  return db;
}

const stores: [string, () => Promise<MusicStore>][] = [
  ["memory", async () => new MemoryMusicStore()],
  ["Jazz", async () => new JazzMusicStore(await openDb(`music-agent-${crypto.randomUUID()}`))],
];

describe.each(stores)("MusicAgent on the %s store", (_name, openStore) => {
  test("streams one assistant turn, records its tool call, and preserves ordering", async () => {
    const store = await openStore();
    const conversation = await store.createConversation("Late-night listening");
    const transcript = await new DeterministicMusicAgent(store).answer(
      conversation,
      "warm saxophone",
    );

    expect(transcript.map((turn) => turn.role)).toEqual(["user", "assistant", "tool"]);
    expect(transcript[1]?.body).toBe(
      "I found a focused listening path for warm saxophone. Starting with the live cut.",
    );
  });

  test("materializes a streamed byte attachment and reads only its requested range", async () => {
    const store = await openStore();
    const conversation = await store.createConversation("Attachment");
    const turnId = await store.addTurn({
      conversationId: conversation,
      role: "user",
      ordinal: 0,
      body: "identify this clip",
    });
    const attachment = await store.addAttachment(
      { turnId, filename: "clip.raw", mediaType: "audio/raw", byteLength: 6 },
      chunks(["ab", "cdef"]),
    );

    expect(Array.from(await store.readAttachmentRange(attachment, 2, 5))).toEqual([99, 100, 101]);
  });

  test("appends multi-byte text without splitting code points", async () => {
    const store = await openStore();
    const conversation = await store.createConversation("Unicode");
    const writer = await store.beginTurn({
      conversationId: conversation,
      role: "assistant",
      ordinal: 0,
    });
    for (const part of ["Café ", "set 🎷", " at 9"]) await writer.append(part);

    const [turn] = await store.transcript(conversation);
    expect(turn?.body).toBe("Café set 🎷 at 9");
  });
});

describe("MusicAgent across synced Jazz clients", () => {
  test("a reader sees an assistant turn grow append by append", async () => {
    const permissions = definePermissions(app, ({ policy }) => {
      for (const table of [
        policy.conversations,
        policy.turns,
        policy.tool_calls,
        policy.attachments,
      ]) {
        table.allowRead.always();
        table.allowInsert.always();
        table.allowUpdate.always();
      }
    });
    server = await startLocalJazzServer({ inMemory: true, schema: app, permissions });
    const writerDb = await openDb(server.appId, server.url);
    const readerDb = await openDb(server.appId, server.url);
    const store = new JazzMusicStore(writerDb);

    const conversation = await store.createConversation("Synced");
    const writer = await store.beginTurn({
      conversationId: conversation,
      role: "assistant",
      ordinal: 0,
    });

    const seen: string[] = [];
    let latest = "";
    let notify: (() => void) | undefined;
    const unsubscribe = readerDb.subscribe(
      app.turns.where({ conversation_id: conversation }),
      (rows) => {
        const body = rows[0]?.body;
        if (body !== undefined && body !== latest) {
          latest = body;
          seen.push(body);
          notify?.();
        }
      },
      { tier: "global" },
    );
    const until = (expected: string) =>
      new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`reader never saw "${expected}"`)), 10_000);
        const check = () => {
          if (latest === expected) {
            clearTimeout(timer);
            resolve();
          }
        };
        notify = check;
        check();
      });

    await writer.append("Booking ");
    await until("Booking ");
    await writer.append("the Blue Note");
    await until("Booking the Blue Note");
    unsubscribe();

    expect(seen).toEqual(["Booking ", "Booking the Blue Note"]);
  });
});
