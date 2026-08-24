import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { createDb, type Db } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
} from "../../../../../../packages/jazz-tools/tests/browser/support.js";
import {
  browserTopologyReporter,
  runTopologyScenario,
} from "../../../../../../packages/jazz-tools/tests/browser/topology-harness.js";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { bandChatFixtureUsers } from "../../src/fixture.js";
import { createSmokeScenario } from "../../src/scenario.js";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    jazzBandChatBootstrapProfile: (
      server: { appId: string; serverUrl: string },
      userId: string,
      displayName: string,
    ) => Promise<void>;
  }
}

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

/**
 * Adopter-level receipt for two public browser clients connected to one core.
 * The shared harness supplies phase/fault timeouts and receipts; this app-owned
 * workload deliberately keeps its schema and assertions local.
 */
describe("BandChat cross-topology recovery", () => {
  it("converges concurrent messages, reactions, attachments, and an offline replay exactly once", async () => {
    const scenario = createSmokeScenario();
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 29);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>> | undefined;
    let owner: Db | undefined;
    let peer: Db | undefined;
    let ownerProfile: { id: string } | undefined;
    let peerProfile: { id: string } | undefined;
    let room: { id: string } | undefined;
    let subscription: string[] = [];
    let ownerMessage: { id: string } | undefined;
    let peerMessage: { id: string } | undefined;
    let attachmentMessage: { id: string } | undefined;
    let offlineMessage: { id: string } | undefined;
    const receipt = await runTopologyScenario(
      {
        id: scenario.id,
        topology: ["browser", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 29,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/band-chat/apps/nextjs-betterauth test:browser:focused -- tests/browser/topology.e2e.test.tsx`,
        targets: {
          owner: {
            disconnect: async () => owner!.disconnect(),
            reconnect: async () => owner!.reconnect(),
          },
          authorization: {
            failure: async () => {
              const token = await getJazzServerJwtForUser(
                bandChatFixtureUsers.outsider,
                undefined,
                server!.appId,
              );
              const outsider = await openClient(server!, "outsider", token);
              await expect(
                outsider
                  .insert(app.messages, {
                    roomId: room!.id,
                    senderId: ownerProfile!.id,
                    text: "planted unauthorized topology write",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        },
        phases: [
          {
            name: "server JWT bootstrap and peer admission",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("band-chat-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [ownerToken, peerToken] = await Promise.all([
                getJazzServerJwtForUser(bandChatFixtureUsers.owner, undefined, server.appId),
                getJazzServerJwtForUser(bandChatFixtureUsers.peer, undefined, server.appId),
              ]);
              owner = await openClient(server, "owner", ownerToken);
              peer = await openClient(server, "peer", peerToken);
              // Profiles are intentionally provisioned by the trusted Better Auth
              // backend, not by the JWT-bearing browser clients.
              const untrustedOwner = await openClient(server, "untrusted-owner", ownerToken);
              await expect(
                untrustedOwner
                  .insert(app.profiles, {
                    userId: bandChatFixtureUsers.owner,
                    displayName: "forged browser profile",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/permission_denied/i);
              await commands.jazzBandChatBootstrapProfile(
                server,
                bandChatFixtureUsers.owner,
                "Owner",
              );
              await commands.jazzBandChatBootstrapProfile(
                server,
                bandChatFixtureUsers.peer,
                "Peer",
              );
              ownerProfile = await owner.one(
                app.profiles.where({ userId: bandChatFixtureUsers.owner }),
                { tier: "edge" },
              );
              peerProfile = await peer.one(
                app.profiles.where({ userId: bandChatFixtureUsers.peer }),
                {
                  tier: "edge",
                },
              );
              if (!ownerProfile || !peerProfile)
                throw new Error("trusted profile bootstrap did not persist");
              expect(ownerProfile).toMatchObject({ userId: bandChatFixtureUsers.owner });
              expect(peerProfile).toMatchObject({ userId: bandChatFixtureUsers.peer });
              room = (
                await owner
                  .insert(app.rooms, { name: scenario.assertion.visibleText })
                  .wait({ tier: "edge" })
              ).value;
              await owner
                .insert(app.roomMembers, { roomId: room.id, userId: bandChatFixtureUsers.owner })
                .wait({ tier: "edge" });
              await owner
                .insert(app.roomMembers, { roomId: room.id, userId: bandChatFixtureUsers.peer })
                .wait({ tier: "edge" });
              await waitForQuery(
                peer,
                app.rooms.where({ id: room.id }),
                (rooms) => rooms.length === 1,
                "peer receives invitation",
                15_000,
                "edge",
              );
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "two-client reaction and attachment delivery",
            run: async () => {
              const unsubscribe = peer!.subscribeAll(
                app.messages
                  .where({ roomId: room!.id })
                  .select("*", "$createdAt")
                  .orderBy("$createdAt", "asc"),
                (snapshot) => {
                  subscription = snapshot.all.map((message) => message.id);
                },
              );
              ctx.trackSubscription(unsubscribe);
              const ownerInsert = owner!.insert(app.messages, {
                roomId: room!.id,
                senderId: ownerProfile!.id,
                text: "owner concurrent",
              });
              const [created, peerCreated] = await Promise.all([
                ownerInsert.wait({ tier: "edge" }),
                peer!
                  .insert(app.messages, {
                    roomId: room!.id,
                    senderId: peerProfile!.id,
                    text: "peer concurrent",
                  })
                  .wait({ tier: "edge" }),
              ]);
              ownerMessage = created.value;
              peerMessage = peerCreated.value;
              await Promise.all([
                owner!
                  .insert(app.reactions, {
                    messageId: ownerMessage.id,
                    userId: bandChatFixtureUsers.owner,
                    emoji: "🎸",
                  })
                  .wait({ tier: "edge" }),
                peer!
                  .insert(app.reactions, {
                    messageId: ownerMessage.id,
                    userId: bandChatFixtureUsers.peer,
                    emoji: "🔥",
                  })
                  .wait({ tier: "edge" }),
                owner!
                  .insert(app.messages, {
                    roomId: room!.id,
                    senderId: ownerProfile!.id,
                    text: "inline attachment",
                    attachment: new Uint8Array([1, 2, 3]),
                    attachmentName: "setlist.txt",
                  })
                  .wait({ tier: "edge" })
                  .then((receipt) => {
                    attachmentMessage = receipt.value;
                  }),
              ]);
            },
            faultsAfter: [{ kind: "disconnect", target: "owner" }],
          },
          {
            name: "offline local write",
            run: async () => {
              const offline = owner!.insert(app.messages, {
                roomId: room!.id,
                senderId: ownerProfile!.id,
                text: "offline replay",
              });
              offlineMessage = (await offline.wait({ tier: "local" })).value;
              expect(
                (await owner!.all(app.messages.where({ roomId: room!.id }))).some(
                  (message) => message.text === "offline replay",
                ),
              ).toBe(true);
            },
            faultsAfter: [{ kind: "reconnect", target: "owner" }],
          },
          {
            name: "peer convergence after reconnect",
            run: async () => {
              const messages = await waitForQuery(
                peer!,
                app.messages
                  .where({ roomId: room!.id })
                  .select("*", "$createdAt")
                  .orderBy("$createdAt", "asc"),
                (rows) => rows.length === 4,
                "peer convergence after reconnect",
                20_000,
                "edge",
              );
              await waitForCondition(
                async () => subscription.length === messages.length,
                10_000,
                "peer subscription did not converge",
              );
              expect(subscription).toEqual(messages.map((message) => message.id));
              expect(new Set(messages.map((message) => message.id))).toEqual(
                new Set([
                  ownerMessage!.id,
                  peerMessage!.id,
                  attachmentMessage!.id,
                  offlineMessage!.id,
                ]),
              );
              expect(
                messages.find((message) => message.id === attachmentMessage!.id),
              ).toMatchObject({
                text: "inline attachment",
                attachment: new Uint8Array([1, 2, 3]),
                attachmentName: "setlist.txt",
              });
              expect(
                (
                  await peer!.all(app.reactions.where({ messageId: ownerMessage!.id }), {
                    tier: "edge",
                  })
                )
                  .map((reaction) => reaction.emoji)
                  .sort(),
              ).toEqual(["🎸", "🔥"]);
            },
          },
        ],
        cleanup: async () => ctx.cleanup(),
        cleanupTimeoutMs: 10_000,
      },
      browserTopologyReporter,
    );
    expect(receipt).toMatchObject({
      status: "passed",
      seed: Number.isSafeInteger(seed) ? seed : 29,
    });
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
    ]);
  }, 75_000);

  // #1844 (reproducing PRs #1830 and #1838): do not convert this to an inline
  // fixture or skip it. It is the adopter-facing receipt for indirect large-value
  // materialization at a receiving browser through the shared server path.
  it.fails("materializes an indirect attachment at the receiving browser (#1844)", async () => {
    const server = await getJazzServerInfo(uniqueDbName("band-chat-indirect-bytes"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const [ownerToken, peerToken] = await Promise.all([
      getJazzServerJwtForUser(bandChatFixtureUsers.largeOwner, undefined, server.appId),
      getJazzServerJwtForUser(bandChatFixtureUsers.largePeer, undefined, server.appId),
    ]);
    const owner = await openClient(server, "large-owner", ownerToken);
    const peer = await openClient(server, "large-peer", peerToken);
    await commands.jazzBandChatBootstrapProfile(server, bandChatFixtureUsers.largeOwner, "Owner");
    const profile = await owner.one(
      app.profiles.where({ userId: bandChatFixtureUsers.largeOwner }),
      { tier: "edge" },
    );
    if (!profile) throw new Error("trusted profile bootstrap did not persist");
    const room = (
      await owner.insert(app.rooms, { name: "large-value receipt" }).wait({ tier: "edge" })
    ).value;
    await owner
      .insert(app.roomMembers, { roomId: room.id, userId: bandChatFixtureUsers.largeOwner })
      .wait({ tier: "edge" });
    await owner
      .insert(app.roomMembers, { roomId: room.id, userId: bandChatFixtureUsers.largePeer })
      .wait({ tier: "edge" });
    const bytes = new Uint8Array(256 * 1024).fill(7);
    await owner
      .insert(app.messages, {
        roomId: room.id,
        senderId: profile.id,
        text: "indirect attachment",
        attachment: bytes,
        attachmentName: "tour-notes.bin",
      })
      .wait({ tier: "edge" });
    const [received] = await waitForQuery(
      peer,
      app.messages.where({ roomId: room.id }),
      (messages) => messages.length === 1,
      "peer receives indirect attachment",
      20_000,
      "edge",
    );
    expect(received.attachment).toEqual(bytes);
  });
});

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName: uniqueDbName(`band-chat-${label}`) },
    }),
  );
}
