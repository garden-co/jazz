import { afterEach, describe, expect, test } from "vitest";
import { createJazzClient, type Db } from "jazz-tools";
import permissions from "../permissions.js";
import { startTestJazz, type TestJazz } from "jazz-tools/testing";
import { app } from "../schema.js";
import { memoryAccountStore } from "../../../../testing/accounts.js";
import {
  DeterministicMusicAgent,
  JazzMusicStore,
  MemoryMusicStore,
  type MusicStore,
} from "../src/music-agent.js";

const sessions: Awaited<ReturnType<typeof createJazzClient>>[] = [];

/** Bytes columns stream Uint8Array chunks (text columns take strings). */
async function* byteChunks(parts: readonly string[]): AsyncIterable<Uint8Array> {
  for (const part of parts) yield new TextEncoder().encode(part);
}
let server: TestJazz | undefined;

afterEach(async () => {
  await Promise.all(sessions.splice(0).map((session) => session.close()));
  await server?.stop();
  server = undefined;
});

const loadLocalFirst = () => import("jazz-tools/local-first");

/**
 * A real device account of its own on an in-memory local-first Jazz runtime:
 * on this device only, or syncing through the app backend at `serverUrl`.
 */
async function openDb(serverUrl: string | null = null): Promise<Db> {
  const session = await createJazzClient({
    serverUrl,
    initial: "device",
    store: memoryAccountStore(),
    driver: { type: "memory" },
    client: loadLocalFirst,
    localFirst: "now",
  });
  sessions.push(session);
  return session.db;
}

const stores: [string, () => Promise<MusicStore>][] = [
  ["memory", async () => new MemoryMusicStore()],
  ["Jazz", async () => new JazzMusicStore(await openDb())],
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
      byteChunks(["ab", "cdef"]),
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
    // Guests sync through the app's backend: a core admits only its backend.
    server = await startTestJazz({ app, permissions });
    const writerDb = await openDb(server.url);
    const readerDb = await openDb(server.url);
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
      { tier: "remote" },
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
