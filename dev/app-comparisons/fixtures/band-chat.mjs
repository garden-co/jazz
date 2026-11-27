// BandChat fixture (the hero benchmark's shape): 256 rooms, 10,000 messages,
// one busy room. Users "seed" (creates every room), "a" and "b" (the
// harness's two signed-in users, members of every room).
import { rng, pick } from "./rng.mjs";

export const USERS = {
  seed: { name: "Seed Bandmate", email: "seed@bench.invalid", password: "bench-password-seed" },
  a: { name: "Bench A", email: "a@bench.invalid", password: "bench-password-a" },
  b: { name: "Bench B", email: "b@bench.invalid", password: "bench-password-b" },
};
export const ROOMS = 256;
export const MESSAGES = 10_000;
export const BUSY_ROOM = "room-0"; // "Busy room": 2,000 of the messages

const WORDS = "the set list soundcheck van load tonight encore riff tempo bass drums mic stage merch".split(" ");

export function bandChatFixture() {
  const r = rng(2);
  const t0 = Date.UTC(2026, 10, 1);
  const rooms = Array.from({ length: ROOMS }, (_, i) => ({
    key: `room-${i}`,
    name: i === 0 ? "Busy room" : `Room ${i + 1}`,
    createdAtMs: t0 + i,
  }));
  const members = rooms.flatMap((room) => ["seed", "a", "b"].map((user) => ({ roomKey: room.key, user })));
  const messages = [];
  for (let i = 0; i < MESSAGES; i++) {
    const roomKey = i < 2000 ? "room-0" : `room-${1 + (i % (ROOMS - 1))}`;
    const length = 3 + Math.floor(r() * 12);
    messages.push({
      key: `msg-${i}`,
      roomKey,
      sender: pick(r, ["seed", "seed", "a", "b"]),
      text: Array.from({ length }, () => pick(r, WORDS)).join(" "),
      createdAtMs: t0 + 10_000 + i * 1000,
    });
  }
  return { users: USERS, rooms, members, messages };
}
