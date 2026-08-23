import { afterAll, beforeAll, bench, describe } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { seedBandChat } from "./fixture.js";
import { app } from "./schema.js";

let db: Db;
let roomId: string;
let senderId: string;

beforeAll(async () => {
  db = await createDb({ appId: "band-chat-benchmarks", driver: { type: "memory" } });
  const fixture = seedBandChat(db);
  roomId = fixture.rooms[0]!.id;
  senderId = fixture.profiles[0]!.id;
});
afterAll(async () => db.shutdown());

describe("band-chat/deterministic", () => {
  bench("room-list/materialize-24", async () => {
    await db.all(app.rooms.select("id", "name").orderBy("name", "asc"));
  });

  bench("message-window/include-sender/materialize-40", async () => {
    await db.all(
      app.messages
        .where({ roomId })
        .select("*", "$createdAt")
        .include({ sender: true })
        .orderBy("$createdAt", "desc")
        .limit(40),
    );
  });

  bench("message-churn/insert-32-rollback", () => {
    const tx = db.beginTransaction();
    for (let index = 0; index < 32; index++)
      tx.insert(app.messages, { roomId, senderId, text: `churn-${index}` });
    tx.rollback();
  });
});
