import { afterEach, describe, expect, it } from "vitest";
import { createJazzClient, type Db, type JazzClientConfig } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue";
import {
  TestCleanup,
  uniqueDbName,
  waitForCondition,
} from "../../../../../../packages/jazz-tools/tests/browser/support";
import {
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  startJazzApp,
  type JazzServerInfo,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server";
import permissions from "../../permissions";
import { app } from "../../schema";
import { memoryAccountStore } from "../../../../../testing/accounts";

const cleanup = new TestCleanup();
afterEach(async () => cleanup.cleanup());

interface Member {
  db: Db;
  author: string;
  profileId: string;
}

interface Live<T> {
  rows: T[];
  errors: string[];
}

/**
 * garden-co/jazz#3815, in the order the UI performs it. The room already holds
 * a streamed attachment. The guest's dashboard has its room list and
 * memberships subscribed when it asks to join; the creator admits it
 * (membership insert + request delete in one transaction); the guest opens the
 * room; then both sides post, each post also bumping the room's activity.
 */
describe("BandChat admission with open subscriptions", () => {
  it("keeps room history and the room list live for an admitted guest", async () => {
    const core = await getJazzServerInfo(uniqueDbName("band-chat-admission"));
    await deploy({
      appId: core.appId,
      serverUrl: core.serverUrl,
      adminSecret: core.adminSecret,
      schema: app,
      permissions,
    });
    const server = await startJazzApp(core, { schema: app, permissions });
    const owner = await openMember(server, "owner");
    const guest = await openMember(server, "guest");

    const room = await owner.db.insert(app.rooms, { name: "Rehearsal" }).wait({ tier: "global" });
    await owner.db
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: owner.author,
        memberProfileId: owner.profileId,
      })
      .wait({ tier: "global" });
    await post(owner, room.id, "Soundcheck moved to 7.");
    // Composer's attachment path: the file streams into its own message row.
    const bytes = new TextEncoder().encode("1. Opener\n2. Ballad\n");
    const attachment = await owner.db.insertStreaming(app.messages, {
      roomId: room.id,
      senderId: owner.profileId,
      text: "",
      attachmentName: "setlist.txt",
      attachmentType: "text/plain",
      attachmentSize: bytes.byteLength,
      attachment: new Blob([bytes]).stream(),
    });
    await attachment.wait({ tier: "global" });

    // What the guest's dashboard has open before it asks to join.
    const guestRooms = live(guest.db, app.rooms.select("*", "$createdBy", "$createdAt"));
    const guestMemberships = live(guest.db, app.roomMembers.where({ memberAuthor: guest.author }));

    const request = await guest.db
      .insert(app.joinRequests, {
        roomId: room.id,
        requester: guest.author,
        profileId: guest.profileId,
      })
      .wait({ tier: "global" });
    // MembersDialog admits and clears the request in one transaction.
    const admission = await owner.db.transaction((tx) => {
      tx.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guest.author,
        memberProfileId: guest.profileId,
      });
      tx.delete(app.joinRequests, request.id);
    });
    await admission.wait({ tier: "global" });
    await until(
      async () =>
        guestMemberships.rows.some((membership) => membership.roomId === room.id) &&
        guestRooms.rows.some((row) => row.id === room.id),
      15_000,
      "guest sees its membership and the room",
      guestRooms,
      guestMemberships,
    );

    // The guest opens the room, as RoomView does once the membership exists.
    const guestHistory = live(guest.db, roomHistory(room.id));
    const ownerHistory = live(owner.db, roomHistory(room.id));
    await until(
      async () => guestHistory.rows.length === 2,
      15_000,
      "guest receives the room history",
      guestRooms,
      guestMemberships,
      guestHistory,
    );

    await post(owner, room.id, "Bring the in-ear packs.");
    await until(
      async () => texts(guestHistory).includes("Bring the in-ear packs."),
      15_000,
      "guest receives the creator's next message live",
      guestRooms,
      guestMemberships,
      guestHistory,
    );
    await post(guest, room.id, "In. I'll bring the spare snare too.");
    await until(
      async () => texts(ownerHistory).includes("In. I'll bring the spare snare too."),
      15_000,
      "creator receives the guest's reply",
      guestRooms,
      guestMemberships,
      guestHistory,
    );
    await until(
      async () => guestRooms.rows.find((row) => row.id === room.id)?.lastActivityAt != null,
      15_000,
      "guest's room list follows the room's activity",
      guestRooms,
      guestMemberships,
      guestHistory,
    );
    expect({
      rooms: guestRooms.errors,
      memberships: guestMemberships.errors,
      history: guestHistory.errors,
    }).toEqual({ rooms: [], memberships: [], history: [] });
  });
});

/** Like waitForCondition, but a timeout names every subscription error seen. */
async function until(
  check: () => Promise<boolean>,
  timeoutMs: number,
  label: string,
  ...subscriptions: Live<unknown>[]
) {
  try {
    await waitForCondition(check, timeoutMs, label);
  } catch (cause) {
    const errors = subscriptions.flatMap((subscription) => subscription.errors);
    throw new Error(`${label}; subscription errors: ${JSON.stringify(errors)}`, { cause });
  }
}

/** RoomView's history query: newest page first, without attachment bytes. */
function roomHistory(roomId: string) {
  return app.messages
    .where({ roomId })
    .select("id", "roomId", "senderId", "text", "attachmentName", "canvasId", "$createdAt")
    .orderBy("$createdAt", "desc")
    .limit(50);
}

function texts(history: Live<{ text: string }>) {
  return history.rows.map((message) => message.text);
}

/** Composer's send: the message and the room's activity commit together. */
async function post(member: Member, roomId: string, text: string) {
  const posting = await member.db.transaction((tx) => {
    tx.insert(app.messages, { roomId, senderId: member.profileId, text });
    tx.update(app.rooms, roomId, { lastActivityAt: new Date() });
  });
  await posting.wait({ tier: "global" });
}

function live<T extends { id: string }>(db: Db, query: Parameters<Db["subscribe"]>[0]): Live<T> {
  const state: Live<T> = { rows: [], errors: [] };
  const stop = db.subscribe(query as never, {
    onUpdate: (rows: T[]) => {
      state.rows = rows;
    },
    onError: (error: unknown) => {
      state.errors.push(error instanceof Error ? error.message : String(error));
    },
  });
  cleanup.trackSubscription(stop);
  return state;
}

const loadLocalFirst = () => import("jazz-tools/local-first");

/**
 * Sign `userId` in on their own origin of the app backend, and open their
 * local-first client: a session of its own (its own account store), closed
 * with the test's cleanup.
 */
async function openSession(
  server: JazzServerInfo,
  userId: string,
  config: Partial<JazzClientConfig>,
) {
  const session = await createJazzClient({
    serverUrl: await jazzAppUserUrl(server, userId),
    fetch: jazzAppFetch,
    store: memoryAccountStore(),
    client: loadLocalFirst,
    // These tests write offline and read the local tiers: local-first from the start.
    localFirst: "now",
    ...config,
  });
  cleanup.trackTransport({ stop: () => session.close() });
  return { db: session.db, author: session.getSnapshot().user!.accountId };
}

async function openMember(server: JazzServerInfo, userId: string): Promise<Member> {
  const { db, author } = await openSession(server, userId, {
    driver: { type: "persistent", dbName: uniqueDbName(`band-chat-${userId}`) },
  });
  const profile = await db
    .insert(app.profiles, { author, displayName: userId })
    .wait({ tier: "global" });
  return { db, author, profileId: profile.id };
}
