import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue";
import {
  TestCleanup,
  uniqueDbName,
  waitForQuery,
} from "../../../../../../packages/jazz-tools/tests/browser/support";
import {
  browserTopologyReporter,
  runTopologyScenario,
} from "../../../../../../packages/jazz-tools/tests/browser/topology-harness";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server";
import permissions from "../../permissions";
import { app } from "../../schema";
import { authorForSession } from "../../src/lib/identity";

const cleanup = new TestCleanup();
afterEach(async () => cleanup.cleanup());

interface ClientIdentity {
  db: Db;
  author: string;
  profileId: string;
}

/** Decode only the test issuer used to construct the canonical Jazz author. */
function authorFromTestToken(token: string, userId: string): string {
  const claims = JSON.parse(atob(token.split(".")[1]!)) as { iss: string };
  return authorForSession(claims.iss, userId);
}

/**
 * Two issuer-scoped members concurrently create messages, reactions, and an
 * attachment; one member then writes locally while disconnected and replays it.
 *
 * owner ──message/reaction/attachment──► core ◄──message/reaction── peer
 *   └──offline message──disconnect/reconnect──► core ─────────────► peer
 */
describe("BandChat cross-topology recovery", () => {
  it("converges concurrent room activity and replays one offline message", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 29);
    let owner: ClientIdentity | undefined;
    let peer: ClientIdentity | undefined;
    let roomId: string | undefined;
    let ownerMessageId: string | undefined;
    let peerMessageId: string | undefined;
    let attachmentMessageId: string | undefined;
    let offlineMessageId: string | undefined;
    const receipt = await runTopologyScenario(
      {
        id: "band-chat.topology.concurrent-offline-replay",
        topology: ["browser", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 29,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/band-chat/apps/nextjs-betterauth test:browser -- tests/browser/topology.e2e.test.tsx`,
        targets: {
          owner: {
            disconnect: async () => owner!.db.disconnect(),
            reconnect: async () => owner!.db.reconnect(),
          },
          authorization: {
            // Planted sensitivity: a member cannot forge another issuer-scoped
            // author's reaction.
            failure: async () => {
              await expect(
                peer!.db
                  .insert(app.reactions, {
                    messageId: ownerMessageId!,
                    author: owner!.author,
                    emoji: "forged",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/permission_denied/i);
            },
          },
        },
        phases: [
          {
            name: "issuer-scoped room admission",
            run: async () => {
              const server = await getJazzServerInfo(uniqueDbName("band-chat-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [ownerToken, peerToken] = await Promise.all([
                getJazzServerJwtForUser("owner", undefined, server.appId),
                getJazzServerJwtForUser("peer", undefined, server.appId),
              ]);
              owner = await openMember(server, "owner", ownerToken);
              peer = await openMember(server, "peer", peerToken);
              const room = await owner.db.insert(app.rooms, { name: "Topology rehearsal" }).wait({
                tier: "edge",
              });
              roomId = room.id;
              await owner.db
                .insert(app.roomMembers, { roomId, memberAuthor: owner.author })
                .wait({ tier: "edge" });
              await owner.db
                .insert(app.roomMembers, { roomId, memberAuthor: peer.author })
                .wait({ tier: "edge" });
              await waitForQuery(
                peer.db,
                app.rooms.where({ id: roomId }),
                (rooms) => rooms.length === 1,
                "peer receives owner invitation",
                15_000,
                "edge",
              );
            },
          },
          {
            name: "concurrent messages reactions and attachment",
            run: async () => {
              const [ownerMessage, peerMessage, attachmentMessage] = await Promise.all([
                owner!.db
                  .insert(app.messages, {
                    roomId: roomId!,
                    senderId: owner!.profileId,
                    text: "owner concurrent",
                  })
                  .wait({ tier: "edge" }),
                peer!.db
                  .insert(app.messages, {
                    roomId: roomId!,
                    senderId: peer!.profileId,
                    text: "peer concurrent",
                  })
                  .wait({ tier: "edge" }),
                owner!.db
                  .insert(app.messages, {
                    roomId: roomId!,
                    senderId: owner!.profileId,
                    text: "setlist attachment",
                    attachment: new Uint8Array([1, 2, 3]),
                    attachmentName: "setlist.txt",
                  })
                  .wait({ tier: "edge" }),
              ]);
              ownerMessageId = ownerMessage.id;
              peerMessageId = peerMessage.id;
              attachmentMessageId = attachmentMessage.id;
              await waitForQuery(
                peer!.db,
                app.messages.where({ id: ownerMessageId }),
                (messages) => messages.length === 1,
                "peer observes the message before reacting to it",
                15_000,
                "edge",
              );
              await Promise.all([
                owner!.db
                  .insert(app.reactions, {
                    messageId: ownerMessageId,
                    author: owner!.author,
                    emoji: "🎸",
                  })
                  .wait({ tier: "edge" }),
                peer!.db
                  .insert(app.reactions, {
                    messageId: ownerMessageId,
                    author: peer!.author,
                    emoji: "🔥",
                  })
                  .wait({ tier: "edge" }),
              ]);
            },
            faultsAfter: [
              { kind: "failure", target: "authorization" },
              { kind: "disconnect", target: "owner" },
            ],
          },
          {
            name: "offline local message",
            run: async () => {
              const offline = owner!.db.insert(app.messages, {
                roomId: roomId!,
                senderId: owner!.profileId,
                text: "offline replay",
              });
              await offline.wait({ tier: "local" });
              offlineMessageId = offline.value.id;
              expect(
                (await owner!.db.all(app.messages.where({ roomId: roomId! }))).some(
                  (message) => message.id === offlineMessageId,
                ),
              ).toBe(true);
            },
            faultsAfter: [{ kind: "reconnect", target: "owner" }],
          },
          {
            name: "peer convergence after replay",
            run: async () => {
              const messages = await waitForQuery(
                peer!.db,
                app.messages.where({ roomId: roomId! }).select("*", "$createdAt"),
                (rows) => rows.length === 4,
                "peer receives concurrent and replayed messages exactly once",
                20_000,
                "edge",
              );
              expect(new Set(messages.map((message) => message.id))).toEqual(
                new Set([ownerMessageId, peerMessageId, attachmentMessageId, offlineMessageId]),
              );
              expect(messages.find((message) => message.id === attachmentMessageId)).toMatchObject({
                attachment: new Uint8Array([1, 2, 3]),
                attachmentName: "setlist.txt",
              });
              const reactions = await waitForQuery(
                peer!.db,
                app.reactions.where({ messageId: ownerMessageId! }),
                (rows) => rows.length === 2,
                "peer receives both member reactions",
                15_000,
                "edge",
              );
              expect(reactions.map((reaction) => reaction.emoji).sort()).toEqual(["🎸", "🔥"]);
            },
          },
        ],
        cleanup: async () => cleanup.cleanup(),
        cleanupTimeoutMs: 10_000,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
    ]);
  }, 75_000);

  /**
   * A member keeps only a two-row projected message window after reconnect;
   * deleting that member's room access rejects its next server-authorized write.
   *
   * owner ──four messages──► core ──latest two──► peer
   * owner ──remove membership──► core ──reject next peer write──► peer
   */
  it("keeps a bounded projection through reconnect and rejects writes after revocation", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 41);
    let owner: ClientIdentity | undefined;
    let peer: ClientIdentity | undefined;
    let roomId: string | undefined;
    let peerMembershipId: string | undefined;
    const window = () =>
      app.messages.where({ roomId: roomId! }).select("id", "text").orderBy("text", "desc").limit(2);
    const receipt = await runTopologyScenario(
      {
        id: "band-chat.topology.bounded-window-revocation",
        topology: ["browser", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 41,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/band-chat/apps/nextjs-betterauth test:browser -- tests/browser/topology.e2e.test.tsx`,
        targets: {
          peer: {
            disconnect: async () => peer!.db.disconnect(),
            reconnect: async () => peer!.db.reconnect(),
          },
        },
        phases: [
          {
            name: "member admission",
            run: async () => {
              const server = await getJazzServerInfo(uniqueDbName("band-chat-window"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [ownerToken, peerToken] = await Promise.all([
                getJazzServerJwtForUser("window-owner", undefined, server.appId),
                getJazzServerJwtForUser("window-peer", undefined, server.appId),
              ]);
              owner = await openMember(server, "window-owner", ownerToken);
              peer = await openMember(server, "window-peer", peerToken);
              const room = await owner.db.insert(app.rooms, { name: "Bounded window" }).wait({
                tier: "edge",
              });
              roomId = room.id;
              await owner.db
                .insert(app.roomMembers, { roomId, memberAuthor: owner.author })
                .wait({ tier: "edge" });
              const membership = await owner.db
                .insert(app.roomMembers, { roomId, memberAuthor: peer.author })
                .wait({ tier: "edge" });
              peerMembershipId = membership.id;
            },
          },
          {
            name: "bounded projected delivery",
            run: async () => {
              for (const text of ["first", "second", "third"]) {
                await owner!.db
                  .insert(app.messages, { roomId: roomId!, senderId: owner!.profileId, text })
                  .wait({ tier: "edge" });
              }
              const rows = await waitForQuery(
                peer!.db,
                window(),
                (messages) => messages.length === 2,
                "peer receives two newest projected messages",
                15_000,
                "edge",
              );
              expect(rows.map((message) => message.text)).toEqual(["third", "second"]);
            },
            faultsAfter: [{ kind: "disconnect", target: "peer" }],
          },
          {
            name: "write while projection is offline",
            run: async () => {
              await owner!.db
                .insert(app.messages, {
                  roomId: roomId!,
                  senderId: owner!.profileId,
                  text: "z after reconnect",
                })
                .wait({ tier: "edge" });
              expect(
                (await peer!.db.all(window(), { tier: "local" })).map((row) => row.text),
              ).toEqual(["third", "second"]);
            },
            faultsAfter: [{ kind: "reconnect", target: "peer" }],
          },
          {
            name: "reconnected projection and revoked write",
            run: async () => {
              const rows = await waitForQuery(
                peer!.db,
                window(),
                (messages) => messages.length === 2 && messages[0]?.text === "z after reconnect",
                "peer reconnects with its exact bounded projection",
                15_000,
                "edge",
              );
              expect(rows.map((message) => message.text)).toEqual(["z after reconnect", "third"]);
              await owner!.db.delete(app.roomMembers, peerMembershipId!).wait({ tier: "edge" });
              await expect(
                peer!.db
                  .insert(app.messages, {
                    roomId: roomId!,
                    senderId: peer!.profileId,
                    text: "rejected after revocation",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/permission_denied/i);
            },
          },
        ],
        cleanup: async () => cleanup.cleanup(),
        cleanupTimeoutMs: 10_000,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
  }, 75_000);
});

async function openMember(
  server: { appId: string; serverUrl: string },
  userId: string,
  jwtToken: string,
): Promise<ClientIdentity> {
  const db = cleanup.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName: uniqueDbName(`band-chat-${userId}`) },
    }),
  );
  const author = authorFromTestToken(jwtToken, userId);
  const profile = await db
    .insert(app.profiles, { author, displayName: userId })
    .wait({ tier: "edge" });
  return { db, author, profileId: profile.id };
}
