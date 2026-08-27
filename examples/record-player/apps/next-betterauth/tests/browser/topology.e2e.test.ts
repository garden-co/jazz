import { afterEach, describe, expect, it } from "vitest";
import { createDb, schema as s, type Db, userIdentity } from "jazz-tools";
import type { RowRefValue } from "jazz-tools/permissions";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  TestCleanup,
  uniqueDbName,
  withTimeout,
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
import { betterAuthPermissions, betterAuthSchema } from "../../auth-schema.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import {
  ALBUM_TRACK_LIMIT,
  JazzRecordPlayerStore,
  PLAYLIST_WINDOW_LIMIT,
  PLAYLIST_WINDOW_OFFSET,
} from "../../src/record-player.js";

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

const acknowledgementApp = s.defineApp({
  receipts: s.table({ label: s.string() }),
});
const acknowledgementPermissions = s.definePermissions(acknowledgementApp, ({ policy }) => {
  policy.receipts.allowRead.always();
  policy.receipts.allowInsert.always();
});

// Keep this intentionally separate from RecordPlayer's relational policies:
// it is the smallest external-JWT receipt for a row routed to a recipient by
// an application-owned canonical session user rather than by `$createdBy`.
const recipientApp = s.defineApp({
  invitations: s.table({ subject: s.string(), label: s.string() }),
});
const recipientPermissions = s.definePermissions(recipientApp, ({ policy, session, anyOf }) => {
  policy.invitations.allowRead.where(
    anyOf([{ subject: session.user }, { label: "unmatched control branch" }]),
  );
  policy.invitations.allowInsert.always();
});

// This adds exactly the branch which distinguishes RecordPlayer from the
// scalar control: a correlated owner path through a referenced playlist.
const relationalRecipientApp = s.defineApp({
  ...betterAuthSchema,
  albums: s
    .table({
      title: s.string(),
      artist: s.string(),
      cover_locator: s.string().optional(),
    })
    .indexOnly(["title"]),
  tracks: s
    .table({
      album_id: s.ref("albums"),
      title: s.string(),
      ordinal: s.int(),
      duration_ms: s.int(),
      audio_bytes: s.bytes().optional(),
    })
    .indexOnly(["album_id", "ordinal"]),
  playlists: s.table({ name: s.string() }),
  playlist_entries: s
    .table({
      playlist_id: s.ref("playlists"),
      track_id: s.ref("tracks"),
      position: s.float(),
    })
    .indexOnly(["playlist_id", "position"]),
  invitations: s.table({
    playlist_id: s.ref("playlists"),
    subject: s.string(),
    role: s.enum("listener", "editor"),
    status: s.enum("pending", "accepted", "revoked"),
  }),
  playback_positions: s.table({
    playlist_id: s.ref("playlists"),
    track_id: s.ref("tracks"),
    position_ms: s.int(),
  }),
});
const relationalRecipientPermissions = {
  ...betterAuthPermissions,
  ...s.definePermissions(relationalRecipientApp, ({ policy, session, anyOf, allowedTo }) => {
    policy.albums.allowRead.where({});
    policy.albums.allowInsert.where({ $createdBy: session.user });
    policy.tracks.allowRead.where({});
    policy.tracks.allowInsert.where({ $createdBy: session.user });
    const canEditPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.user },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          subject: session.user,
          role: "editor",
          status: "accepted",
        }),
      ]);
    const canReadPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.user },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          subject: session.user,
          status: "accepted",
        }),
      ]);
    policy.playlists.allowRead.where((playlist) => canReadPlaylist(playlist.id));
    policy.playlists.allowInsert.always();
    policy.playlists.allowUpdate.where({ $createdBy: session.user });
    policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
    policy.playlist_entries.allowInsert.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowUpdate.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowDelete.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.invitations.allowRead.where((invite) =>
      anyOf([
        { subject: session.user },
        policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
      ]),
    );
    policy.invitations.allowInsert.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    policy.invitations.allowUpdate.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    policy.invitations.allowUpdate
      .whereOld({ subject: session.user, status: "pending" })
      .whereNew((invite) =>
        policy.invitations.exists.where({
          id: invite.id,
          playlist_id: invite.playlist_id,
          subject: invite.subject,
          role: invite.role,
          status: "pending",
        }),
      )
      .whereNew({ subject: session.user, status: "accepted" });
    policy.invitations.allowDelete.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    policy.playback_positions.allowRead.where({ $createdBy: session.user });
    policy.playback_positions.allowInsert.always();
    policy.playback_positions.allowUpdate
      .whereOld({ $createdBy: session.user })
      .whereNew({ $createdBy: session.user });
    policy.playback_positions.allowDelete.where({ $createdBy: session.user });
  }),
};

describe("RecordPlayer authenticated playlist topology", () => {
  it("keeps the phased recipient control schema and access paths identical to RecordPlayer", () => {
    expect(relationalRecipientApp.wasmSchema).toEqual(app.wasmSchema);
    expect(relationalRecipientPermissions).toEqual(permissions);
  });

  it("delivers RecordPlayer pending invitations outside the scenario envelope", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-recipient-settlement"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const [ownerToken, recipientToken, secondRecipientToken] = await Promise.all([
      getJazzServerJwtForUser("record-player-recipient-owner", undefined, server.appId),
      getJazzServerJwtForUser("record-player-recipient-editor", undefined, server.appId),
      getJazzServerJwtForUser("record-player-recipient-listener", undefined, server.appId),
    ]);
    const recipientAuthor = canonicalUser(recipientToken);
    const secondRecipientAuthor = canonicalUser(secondRecipientToken);
    const owner = await openClient(server, "recipient-owner", ownerToken);
    const recipient = await openClient(
      server,
      "recipient-editor",
      recipientToken,
      uniqueDbName("record-player-recipient-editor-persistent"),
    );
    const secondRecipient = await openClient(server, "recipient-listener", secondRecipientToken);
    const playlist = await owner
      .insert(app.playlists, {
        name: "recipient settlement receipt",
      })
      .wait({ tier: "edge" });
    const invitation = await owner
      .insert(app.invitations, {
        playlist_id: playlist.id,
        subject: recipientAuthor,
        role: "editor",
        status: "pending",
      })
      .wait({ tier: "edge" });
    const secondInvitation = await owner
      .insert(app.invitations, {
        playlist_id: playlist.id,
        subject: secondRecipientAuthor,
        role: "listener",
        status: "pending",
      })
      .wait({ tier: "edge" });
    await Promise.all([
      waitForQuery(
        recipient,
        app.invitations.where({ subject: recipientAuthor }),
        (rows) => rows[0]?.id === invitation.id && rows[0]?.status === "pending",
        "RecordPlayer recipient observes pending invitation",
        15_000,
        "edge",
      ),
      waitForQuery(
        secondRecipient,
        app.invitations.where({ subject: secondRecipientAuthor }),
        (rows) => rows[0]?.id === secondInvitation.id && rows[0]?.status === "pending",
        "second RecordPlayer recipient observes pending invitation",
        15_000,
        "edge",
      ),
    ]);
  });

  it("settles an observed recipient pending-to-accepted transition at edge", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-acceptance-settlement"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const [ownerToken, recipientToken] = await Promise.all([
      getJazzServerJwtForUser("record-player-acceptance-owner", undefined, server.appId),
      getJazzServerJwtForUser("record-player-acceptance-recipient", undefined, server.appId),
    ]);
    const recipientAuthor = canonicalUser(recipientToken);
    const owner = await openClient(server, "acceptance-owner", ownerToken);
    const recipient = await openClient(server, "acceptance-recipient", recipientToken);
    const playlist = await owner
      .insert(app.playlists, {
        name: "acceptance settlement receipt",
      })
      .wait({ tier: "edge" });
    const invitation = await owner
      .insert(app.invitations, {
        playlist_id: playlist.id,
        subject: recipientAuthor,
        role: "editor",
        status: "pending",
      })
      .wait({ tier: "edge" });
    await waitForQuery(
      recipient,
      app.invitations.where({ subject: recipientAuthor }),
      (rows) => rows[0]?.id === invitation.id && rows[0]?.status === "pending",
      "recipient observes pending invitation before accepting it",
      15_000,
      "edge",
    );
    const acceptance = recipient.update(app.invitations, invitation.id, { status: "accepted" });
    await expect(acceptance.batchId).resolves.toEqual(expect.any(String));
    await withTimeout(
      acceptance.wait({ tier: "edge" }),
      10_000,
      "recipient pending-to-accepted transition did not settle at edge",
    );
  });

  it("settles a RecordPlayer owner playlist at edge outside the scenario envelope", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-owner-settlement"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const token = await getJazzServerJwtForUser(
      "record-player-owner-settlement",
      undefined,
      server.appId,
    );
    const db = await openClient(server, "owner-settlement", token);
    const errors: unknown[] = [];
    const stop = db.onMutationError((event) => errors.push(event));
    const write = db.insert(app.playlists, {
      name: "owner settlement receipt",
    });
    await withTimeout(write.wait({ tier: "local" }), 5_000, "playlist did not settle locally");
    try {
      expect(
        await withTimeout(
          write.wait({ tier: "edge" }),
          10_000,
          `playlist did not settle at edge: ${JSON.stringify(errors)}`,
        ),
      ).toEqual(write.value);
    } finally {
      stop();
    }
  });

  it("settles an identical one-table write at edge", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-ack-probe"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: acknowledgementApp,
      permissions: acknowledgementPermissions,
    });
    const token = await getJazzServerJwtForUser("record-player-probe", undefined, server.appId);
    const db = await openClient(server, "ack-probe", token);
    const errors: unknown[] = [];
    const stop = db.onMutationError((event) => errors.push(event));
    const write = db.insert(acknowledgementApp.receipts, { label: "one-table receipt" });
    await withTimeout(
      write.wait({ tier: "local" }),
      5_000,
      "one-table write did not settle locally",
    );
    try {
      const settled = await withTimeout(
        write.wait({ tier: "edge" }),
        10_000,
        `one-table edge settlement mutationErrors=${JSON.stringify(errors)}`,
      );
      // This is the public WriteResult contract, independent of RecordPlayer
      // policy shape: durability waits on inserts retain the inserted row.
      expect(settled).toEqual(write.value);
    } finally {
      stop();
    }
  }, 30_000);

  it("delivers an external-JWT row through a raw scalar recipient subject", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-recipient-scalar"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: recipientApp,
      permissions: recipientPermissions,
    });
    const [ownerToken, recipientToken] = await Promise.all([
      getJazzServerJwtForUser("record-player-scalar-owner", undefined, server.appId),
      getJazzServerJwtForUser("record-player-scalar-recipient", undefined, server.appId),
    ]);
    const recipientAuthor = canonicalUser(recipientToken);
    const owner = await openClient(server, "scalar-owner", ownerToken);
    const recipient = await openClient(server, "scalar-recipient", recipientToken);
    const invite = await owner
      .insert(recipientApp.invitations, {
        subject: recipientAuthor,
        label: "recipient routing receipt",
      })
      .wait({ tier: "edge" });

    await expect(
      waitForQuery(
        recipient,
        recipientApp.invitations.where({ subject: recipientAuthor }),
        (rows) => rows.length === 1 && rows[0]?.id === invite.id,
        "external-JWT scalar recipient receives invitation",
        15_000,
        "edge",
      ),
    ).resolves.toEqual([
      {
        id: invite.id,
        subject: recipientAuthor,
        label: "recipient routing receipt",
      },
    ]);
  }, 30_000);

  it("delivers a scalar recipient grant alongside a correlated playlist-owner branch", async () => {
    const server = await getJazzServerInfo(uniqueDbName("record-player-recipient-relation"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: relationalRecipientApp,
      permissions: relationalRecipientPermissions,
    });
    const [ownerToken, recipientToken, secondRecipientToken] = await Promise.all([
      getJazzServerJwtForUser("record-player-relation-owner", undefined, server.appId),
      getJazzServerJwtForUser("record-player-relation-recipient", undefined, server.appId),
      getJazzServerJwtForUser("record-player-relation-listener", undefined, server.appId),
    ]);
    const recipientAuthor = canonicalUser(recipientToken);
    const secondRecipientAuthor = canonicalUser(secondRecipientToken);
    const owner = await openClient(server, "relation-owner", ownerToken);
    const recipient = await openClient(server, "relation-recipient", recipientToken);
    const secondRecipient = await openClient(server, "relation-listener", secondRecipientToken);
    const playlist = await owner
      .insert(relationalRecipientApp.playlists, {
        name: "recipient relation receipt",
      })
      .wait({ tier: "edge" });
    const invite = await owner
      .insert(relationalRecipientApp.invitations, {
        playlist_id: playlist.id,
        subject: recipientAuthor,
        role: "listener",
        status: "pending",
      })
      .wait({ tier: "edge" });
    const secondInvite = await owner
      .insert(relationalRecipientApp.invitations, {
        playlist_id: playlist.id,
        subject: secondRecipientAuthor,
        role: "listener",
        status: "pending",
      })
      .wait({ tier: "edge" });

    const receipt = await runTopologyScenario(
      {
        id: "record-player.relational-recipient-control",
        topology: ["browser", "edge", "core"],
        seed: 1,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        targets: {},
        replay: "pnpm --filter record-player-next-betterauth test:browser -- topology.e2e.test.ts",
        phases: [
          {
            name: "recipients observe pending scalar grants",
            run: async () => {
              await expect(
                Promise.all([
                  waitForQuery(
                    recipient,
                    relationalRecipientApp.invitations.where({
                      subject: recipientAuthor,
                    }),
                    (rows) => rows.length === 1 && rows[0]?.id === invite.id,
                    "external-JWT recipient receives scalar grant with correlated owner branch",
                    15_000,
                    "edge",
                  ),
                  waitForQuery(
                    secondRecipient,
                    relationalRecipientApp.invitations.where({
                      subject: secondRecipientAuthor,
                    }),
                    (rows) => rows.length === 1 && rows[0]?.id === secondInvite.id,
                    "second external-JWT recipient receives scalar grant with correlated owner branch",
                    15_000,
                    "edge",
                  ),
                ]),
              ).resolves.toHaveLength(2);
            },
          },
        ],
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
  }, 30_000);

  it("delivers a relational recipient grant when authority setup is one topology phase", async () => {
    const receipt = await runTopologyScenario(
      {
        id: "record-player.relational-recipient-full-phase-control",
        topology: ["browser", "edge", "core"],
        seed: 2,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        targets: {},
        replay: "pnpm --filter record-player-next-betterauth test:browser -- topology.e2e.test.ts",
        phases: [
          {
            name: "create authority and deliver recipient grant",
            run: async () => {
              const server = await getJazzServerInfo(
                uniqueDbName("record-player-recipient-full-phase"),
              );
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: relationalRecipientApp,
                permissions: relationalRecipientPermissions,
              });
              const [ownerToken, recipientToken, listenerToken] = await Promise.all([
                getJazzServerJwtForUser("record-player-phase-owner", undefined, server.appId),
                getJazzServerJwtForUser("record-player-phase-recipient", undefined, server.appId),
                getJazzServerJwtForUser("record-player-phase-listener", undefined, server.appId),
              ]);
              const recipientAuthor = canonicalUser(recipientToken);
              const owner = await openClient(server, "phase-owner", ownerToken);
              const recipient = await openClient(
                server,
                "phase-recipient",
                recipientToken,
                uniqueDbName("record-player-phase-recipient-persistent"),
              );
              await openClient(server, "phase-listener", listenerToken);
              const mutationErrors: unknown[] = [];
              const stopMutationErrors = owner.onMutationError((event) =>
                mutationErrors.push(event),
              );
              const playlistWrite = owner.insert(relationalRecipientApp.playlists, {
                name: "phase relation receipt",
              });
              await withTimeout(
                playlistWrite.wait({ tier: "local" }),
                5_000,
                "relational playlist did not settle locally",
              );
              let playlist: { id: string };
              try {
                playlist = await withTimeout(
                  playlistWrite.wait({ tier: "edge" }),
                  10_000,
                  `relational playlist did not settle at edge: ${JSON.stringify(mutationErrors)}`,
                );
                expect(playlist).toEqual(playlistWrite.value);
              } finally {
                stopMutationErrors();
              }
              const invite = await owner
                .insert(relationalRecipientApp.invitations, {
                  playlist_id: playlist.id,
                  subject: recipientAuthor,
                  role: "listener",
                  status: "pending",
                })
                .wait({ tier: "edge" });
              await waitForQuery(
                recipient,
                relationalRecipientApp.invitations.where({
                  subject: recipientAuthor,
                }),
                (rows) => rows.length === 1 && rows[0]?.id === invite.id,
                "full-phase external-JWT recipient receives scalar grant",
                15_000,
                "edge",
              );
            },
          },
        ],
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
  }, 30_000);

  it("rejects forged acceptance and converges two offline playlist editors exactly", async () => {
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let listener: Db;
    let playlist: { id: string };
    let editorInvite: { id: string };
    let listenerInvite: { id: string };
    let editorToken: string;
    let editorAuthor: string;
    let listenerAuthor: string;
    let editorDbName: string;
    let streamedTrackId: string;
    let streamedTrackAlbumId: string;
    const failedTrackId = "00000000-0000-0000-0000-000000000091";
    const streamedAudioChunks = deterministicAudioChunks();
    const streamedAudioPayload = concatenateBytes(streamedAudioChunks);
    let streamedAlbumTracks: Array<{
      id: string;
      albumId: string;
      title: string;
      ordinal: number;
      durationMs: number;
    }>;
    let belowWindowEntryId: string;
    const seed = Number(import.meta.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 41);

    const receipt = await runTopologyScenario(
      {
        id: "record-player.playlist-auth-reconnect",
        topology: ["browser", "edge", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 41,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/record-player/apps/next-betterauth test:browser -- topology.e2e.test.ts`,
        targets: {
          owner: {
            disconnect: async () => owner.disconnect(),
            reconnect: async () => owner.reconnect(),
          },
          editor: {
            disconnect: async () => editor.disconnect(),
            reconnect: async () => editor.reconnect(),
          },
        },
        phases: [
          {
            name: "publish authority and admit independent sessions",
            run: async () => {
              console.info("[record-player-topology] start server");
              server = await getJazzServerInfo(uniqueDbName("record-player-topology"));
              console.info("[record-player-topology] deploy authority");
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              console.info("[record-player-topology] issue session JWTs");
              const [ownerToken, issuedEditorToken, listenerToken] = await Promise.all([
                getJazzServerJwtForUser("record-player-owner", undefined, server.appId),
                getJazzServerJwtForUser("record-player-editor", undefined, server.appId),
                getJazzServerJwtForUser("record-player-listener", undefined, server.appId),
              ]);
              editorToken = issuedEditorToken;
              editorAuthor = canonicalUser(issuedEditorToken);
              listenerAuthor = canonicalUser(listenerToken);
              editorDbName = uniqueDbName("record-player-editor-persistent");
              console.info("[record-player-topology] open owner edge");
              owner = await openClient(server, "owner", ownerToken);
              console.info("[record-player-topology] open editor edge");
              editor = await openClient(server, "editor", editorToken, editorDbName);
              console.info("[record-player-topology] open listener edge");
              listener = await openClient(server, "listener", listenerToken);
              console.info("[record-player-topology] create playlist");
              const mutationErrors: unknown[] = [];
              const stopMutationErrors = owner.onMutationError((event) => {
                mutationErrors.push(event);
                console.info("[record-player-topology] playlist mutation error", {
                  transactionId: event.transaction.transactionId,
                  code: event.code,
                  reason: event.reason,
                });
              });
              const playlistWrite = owner.insert(app.playlists, {
                name: "Road tape",
              });
              const playlistBatchId = await playlistWrite.batchId;
              console.info("[record-player-topology] playlist transaction", {
                transactionId: playlistBatchId,
              });
              await withTimeout(
                playlistWrite.wait({ tier: "local" }),
                5_000,
                "playlist write did not settle locally",
              );
              console.info("[record-player-topology] playlist local settlement", {
                id: playlistWrite.value.id,
              });
              try {
                const edgeSettlement = playlistWrite.wait({ tier: "edge" });
                void edgeSettlement.then(
                  () =>
                    console.info("[record-player-topology] playlist edge settlement resolved", {
                      transactionId: playlistBatchId,
                    }),
                  (error) =>
                    console.info("[record-player-topology] playlist edge settlement rejected", {
                      transactionId: playlistBatchId,
                      error: String(error),
                    }),
                );
                const settledPlaylist = await withTimeout(
                  edgeSettlement,
                  10_000,
                  `playlist edge settlement mutationErrors=${JSON.stringify(mutationErrors)}`,
                );
                // A WriteResult's durability wait must retain its insert
                // result. In particular, application code can await edge
                // durability before it uses a generated id in a child row.
                expect(settledPlaylist).toEqual(playlistWrite.value);
                playlist = settledPlaylist;
              } finally {
                stopMutationErrors();
              }
              console.info("[record-player-topology] create editor invitation");
              const editorInviteWrite = owner.insert(app.invitations, {
                playlist_id: playlist.id,
                subject: editorAuthor,
                role: "editor",
                status: "pending",
              });
              console.info("[record-player-topology] editor invitation transaction", {
                transactionId: await editorInviteWrite.batchId,
              });
              editorInvite = await editorInviteWrite.wait({ tier: "edge" });
              console.info("[record-player-topology] editor invitation edge settlement");
              console.info("[record-player-topology] create listener invitation");
              const listenerInviteWrite = owner.insert(app.invitations, {
                playlist_id: playlist.id,
                subject: listenerAuthor,
                role: "listener",
                status: "pending",
              });
              console.info("[record-player-topology] listener invitation transaction", {
                transactionId: await listenerInviteWrite.batchId,
              });
              listenerInvite = await listenerInviteWrite.wait({ tier: "edge" });
              console.info("[record-player-topology] listener invitation edge settlement");
              // Edge settlement acknowledges the owner's write; it does not
              // imply either recipient has received the row yet. A recipient
              // accepts an invitation only after its normal read path observes
              // the pending row.
              console.info("[record-player-topology] wait for recipient invitations");
              await Promise.all([
                waitForQuery(
                  editor,
                  app.invitations.where({ subject: editorAuthor }),
                  (rows) => rows[0]?.id === editorInvite.id && rows[0]?.status === "pending",
                  "editor observes pending invitation",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  listener,
                  app.invitations.where({ subject: listenerAuthor }),
                  (rows) => rows[0]?.id === listenerInvite.id && rows[0]?.status === "pending",
                  "listener observes pending invitation",
                  15_000,
                  "edge",
                ),
              ]);
              console.info("[record-player-topology] recipient invitations observed");
              console.info("[record-player-topology] accept editor invitation");
              const editorAcceptance = editor.update(app.invitations, editorInvite.id, {
                status: "accepted",
              });
              const editorAcceptanceBatchId = await editorAcceptance.batchId;
              console.info("[record-player-topology] editor acceptance transaction", {
                transactionId: editorAcceptanceBatchId,
              });
              console.info("[record-player-topology] accept listener invitation");
              const listenerAcceptance = listener.update(app.invitations, listenerInvite.id, {
                status: "accepted",
              });
              const listenerAcceptanceBatchId = await listenerAcceptance.batchId;
              console.info("[record-player-topology] listener acceptance transaction", {
                transactionId: listenerAcceptanceBatchId,
              });
              const stopEditorMutationErrors = editor.onMutationError((event) => {
                console.info("[record-player-topology] editor acceptance error", {
                  transactionId: event.transaction.transactionId,
                  code: event.code,
                  reason: event.reason,
                });
              });
              const stopListenerMutationErrors = listener.onMutationError((event) => {
                console.info("[record-player-topology] listener acceptance error", {
                  transactionId: event.transaction.transactionId,
                  code: event.code,
                  reason: event.reason,
                });
              });
              try {
                const editorAcceptanceSettlement = editorAcceptance.wait({ tier: "edge" });
                const listenerAcceptanceSettlement = listenerAcceptance.wait({ tier: "edge" });
                void editorAcceptanceSettlement.then(
                  () =>
                    console.info("[record-player-topology] editor acceptance settled", {
                      transactionId: editorAcceptanceBatchId,
                    }),
                  (error) =>
                    console.info("[record-player-topology] editor acceptance rejected", {
                      transactionId: editorAcceptanceBatchId,
                      error: String(error),
                    }),
                );
                void listenerAcceptanceSettlement.then(
                  () =>
                    console.info("[record-player-topology] listener acceptance settled", {
                      transactionId: listenerAcceptanceBatchId,
                    }),
                  (error) =>
                    console.info("[record-player-topology] listener acceptance rejected", {
                      transactionId: listenerAcceptanceBatchId,
                      error: String(error),
                    }),
                );
                await Promise.all([editorAcceptanceSettlement, listenerAcceptanceSettlement]);
              } finally {
                stopEditorMutationErrors();
                stopListenerMutationErrors();
              }
              console.info("[record-player-topology] invitation acceptances settled");

              // This uses precisely the public streaming API that the app's
              // persistence adapter uses. The list screens must remain useful
              // without eagerly materializing the audio field.
              console.info("[record-player-topology] create streamed track album");
              const streamedTrackAlbum = await owner
                .insert(app.albums, { title: "A streamed record", artist: "Jazz" })
                .wait({ tier: "edge" });
              streamedTrackAlbumId = streamedTrackAlbum.id;
              // The stream is consumed and committed locally while its peer is
              // deliberately disconnected. Reconnecting may retry transport,
              // but must not duplicate the row or lose/split its byte payload.
              console.info("[record-player-topology] create streamed track offline");
              await owner.disconnect();
              const streamedTrack = await owner.insertStreaming(app.tracks, {
                album_id: streamedTrackAlbum.id,
                title: "Streaming receipt",
                ordinal: 0,
                duration_ms: 2,
                audio_bytes: audioStream(streamedAudioChunks),
              });
              await withTimeout(
                streamedTrack.wait({ tier: "local" }),
                15_000,
                "offline streamed audio track did not settle locally",
              );
              await expect(
                listener.all(app.tracks.where({ id: streamedTrack.value.id }), { tier: "edge" }),
              ).resolves.toEqual([]);
              await delay(100);
              await expect(
                listener.all(app.tracks.where({ id: streamedTrack.value.id }), { tier: "edge" }),
              ).resolves.toEqual([]);
              await owner.reconnect();
              streamedTrackId = (
                await withTimeout(
                  streamedTrack.wait({ tier: "edge" }),
                  15_000,
                  "streamed audio track did not settle at edge",
                )
              ).id;
              // The metadata adapter has a hard page bound. Seed one more
              // row than it is allowed to return so the bound is observable,
              // while retaining this first row as the streamed-audio case.
              const additionalMetadata = await Promise.all(
                Array.from({ length: ALBUM_TRACK_LIMIT }, async (_, index) => {
                  const ordinal = index + 1;
                  const track = await owner
                    .insert(app.tracks, {
                      album_id: streamedTrackAlbumId,
                      title: `Streaming metadata ${ordinal}`,
                      ordinal,
                      duration_ms: ordinal + 2,
                    })
                    .wait({ tier: "edge" });
                  return {
                    id: track.id,
                    albumId: streamedTrackAlbumId,
                    title: `Streaming metadata ${ordinal}`,
                    ordinal,
                    durationMs: ordinal + 2,
                  };
                }),
              );
              streamedAlbumTracks = [
                {
                  id: streamedTrackId,
                  albumId: streamedTrackAlbumId,
                  title: "Streaming receipt",
                  ordinal: 0,
                  durationMs: 2,
                },
                ...additionalMetadata,
              ];
              console.info("[record-player-topology] streamed track settled");

              // Source failure is intentionally tested at the app boundary:
              // a stream that has yielded bytes but then errors must not
              // publish a partly populated track locally or after reconnect.
              await expect(
                owner.insertStreaming(
                  app.tracks,
                  {
                    album_id: streamedTrackAlbum.id,
                    title: "Must not publish",
                    ordinal: 9_999,
                    duration_ms: 1,
                    audio_bytes: failingAudioStream(),
                  },
                  { id: failedTrackId },
                ),
              ).rejects.toThrow("record-player injected audio source failure");
              await expect(
                owner.all(app.tracks.where({ id: failedTrackId }), { tier: "local" }),
              ).resolves.toEqual([]);
              await delay(100);
              await expect(
                owner.all(app.tracks.where({ id: failedTrackId }), { tier: "local" }),
              ).resolves.toEqual([]);

              // Advance both peers through a later accepted edge mutation.
              // Absence after this barrier rules out a merely delayed partial
              // publication from the failed stream.
              await owner.disconnect();
              await owner.reconnect();
              const failureBarrier = await owner
                .insert(app.albums, { title: "Failed-stream barrier", artist: "Jazz" })
                .wait({ tier: "edge" });
              await waitForQuery(
                listener,
                app.albums.where({ id: failureBarrier.id }),
                (rows) => rows[0]?.id === failureBarrier.id,
                "listener advances beyond failed streamed mutation",
                15_000,
                "edge",
              );
              for (const db of [owner, listener]) {
                await expect(
                  db.all(app.tracks.where({ id: failedTrackId }), { tier: "edge" }),
                ).resolves.toEqual([]);
              }
              await delay(100);
              for (const db of [owner, listener]) {
                await expect(
                  db.all(app.tracks.where({ id: failedTrackId }), { tier: "edge" }),
                ).resolves.toEqual([]);
              }
            },
          },
          {
            name: "exercise metadata and window queries used by the rendered screens",
            run: async () => {
              const windowAlbum = await owner
                .insert(app.albums, { title: "Window catalogue", artist: "Jazz" })
                .wait({ tier: "edge" });
              const windowEntries: Array<{ id: string; trackId: string; position: number }> = [];
              for (
                let position = 0;
                position < PLAYLIST_WINDOW_OFFSET + PLAYLIST_WINDOW_LIMIT + 1;
                position += 1
              ) {
                const track = await owner
                  .insert(app.tracks, {
                    album_id: windowAlbum.id,
                    title: `Window ${position}`,
                    ordinal: position,
                    duration_ms: position + 1,
                  })
                  .wait({ tier: "edge" });
                const entry = await owner
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: track.id,
                    position,
                  })
                  .wait({ tier: "edge" });
                windowEntries.push({ id: entry.id, trackId: track.id, position });
              }
              belowWindowEntryId = windowEntries[0]!.id;

              // Keep this query structurally identical to RecordPlayerClient's
              // metadata catalogue hook: title ordering and bounded materialization.
              const catalogue = await waitForQuery(
                listener,
                app.albums.orderBy("title", "asc").limit(20),
                (rows) => rows.length >= 2,
                "listener catalogue visibility",
                15_000,
                "edge",
              );
              expect(catalogue.map((row) => row.title)).toEqual(
                catalogue.map((row) => row.title).sort(),
              );

              // This is the literal playlist-screen access path, including its
              // deliberately nonzero window offset. There is one extra entry
              // beyond the requested page, so removing the bound changes this
              // assertion rather than leaving an equivalent query.
              const expectedRenderedWindow = windowEntries.slice(
                PLAYLIST_WINDOW_OFFSET,
                PLAYLIST_WINDOW_OFFSET + PLAYLIST_WINDOW_LIMIT,
              );
              const visibleWindow = await waitForQuery(
                listener,
                app.playlist_entries
                  .where({ playlist_id: playlist.id })
                  .orderBy("position", "asc")
                  .offset(PLAYLIST_WINDOW_OFFSET)
                  .limit(PLAYLIST_WINDOW_LIMIT),
                (rows) =>
                  rows.length === expectedRenderedWindow.length &&
                  rows.map((row) => row.position).join(",") ===
                    expectedRenderedWindow.map((entry) => entry.position).join(","),
                "listener rendered playlist window",
                15_000,
                "edge",
              );
              expect(visibleWindow.map((row) => row.track_id)).toEqual(
                expectedRenderedWindow.map((entry) => entry.trackId),
              );

              // A metadata-only projection requests no streamed-byte column
              // for a visible catalogue row. The focused store receipt also
              // pins its translated runtime projection below.
              const projectedTrack = await waitForQuery(
                listener,
                app.tracks
                  .where({ id: streamedTrackId })
                  .select("id", "album_id", "title", "ordinal", "duration_ms"),
                (rows) =>
                  rows[0]?.id === streamedTrackId &&
                  rows[0]?.title === "Streaming receipt" &&
                  !Object.hasOwn(rows[0], "audio_bytes"),
                "streamed track metadata without audio materialization",
                15_000,
                "edge",
              );
              expect(Object.hasOwn(projectedTrack[0]!, "audio_bytes")).toBe(false);
              expect("audio_bytes" in projectedTrack[0]!).toBe(false);

              // Playback is the exceptional path that explicitly asks for
              // the bytes. It proves that the offline-created, multi-chunk
              // value arrives intact after reconnect, while the query above
              // proves normal browsing never asks for it.
              const playbackTrack = await waitForQuery(
                listener,
                app.tracks.where({ id: streamedTrackId }),
                (rows) =>
                  rows[0]?.id === streamedTrackId &&
                  rows[0]?.audio_bytes instanceof Uint8Array &&
                  rows[0].audio_bytes.byteLength === streamedAudioPayload.byteLength &&
                  rows[0].audio_bytes[0] === streamedAudioPayload[0] &&
                  rows[0].audio_bytes.at(-1) === streamedAudioPayload.at(-1),
                "listener receives intact streamed audio after owner reconnect",
                15_000,
                "edge",
              );
              expect(playbackTrack[0]!.audio_bytes).toEqual(streamedAudioPayload);

              // The page itself deliberately returns only the bounded prefix,
              // but wait until the extra record has replicated before asking
              // the store for that prefix. Otherwise a lagging peer could make
              // a missing limit look correct by returning a short suffix.
              await waitForQuery(
                listener,
                app.tracks
                  .where({ album_id: streamedTrackAlbumId })
                  .orderBy("ordinal", "asc")
                  .select("id", "album_id", "title", "ordinal", "duration_ms"),
                (rows) => rows.length === ALBUM_TRACK_LIMIT + 1,
                "listener receives the complete streamed-album metadata set",
                15_000,
                "edge",
              );

              // Exercise the app's actual persistence boundary after the
              // observer has received the metadata. This deliberately avoids
              // reading `audio_bytes`: the public list API must keep streamed
              // payloads out of the normal catalogue and playlist paths.
              const listenerStore = new JazzRecordPlayerStore(listener);
              await expect(listenerStore.tracksForAlbum(streamedTrackAlbumId)).resolves.toEqual(
                streamedAlbumTracks.slice(0, ALBUM_TRACK_LIMIT),
              );
              await expect(
                listenerStore.playlistWindow(playlist.id, PLAYLIST_WINDOW_OFFSET, 2),
              ).resolves.toEqual(
                windowEntries
                  .slice(PLAYLIST_WINDOW_OFFSET, PLAYLIST_WINDOW_OFFSET + 2)
                  .map((entry) => ({
                    id: entry.id,
                    trackId: entry.trackId,
                    playlistId: playlist.id,
                    position: entry.position,
                  })),
              );
            },
          },
          {
            name: "plant immutable acceptance and listener-write negatives",
            run: async () => {
              await expect(
                listener
                  .update(app.invitations, listenerInvite.id, {
                    role: "editor",
                    status: "accepted",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
              await expect(
                listener
                  .update(app.invitations, listenerInvite.id, {
                    playlist_id: "00000000-0000-0000-0000-000000000000",
                    status: "accepted",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
              const track = await owner
                .insert(app.tracks, {
                  album_id: (
                    await owner
                      .insert(app.albums, { title: "Receipt", artist: "Jazz" })
                      .wait({ tier: "edge" })
                  ).id,
                  title: "Boundary",
                  ordinal: 0,
                  duration_ms: 1,
                })
                .wait({ tier: "edge" });
              await expect(
                listener
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: track.id,
                    position: 1,
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
              await waitForQuery(
                editor,
                app.invitations.where({ id: editorInvite.id }),
                (rows) => rows[0]?.role === "editor" && rows[0]?.playlist_id === playlist.id,
                "rejected acceptance rolls back",
                15_000,
                "edge",
              );
            },
            faultsAfter: [
              { kind: "disconnect", target: "owner" },
              { kind: "disconnect", target: "editor" },
            ],
          },
          {
            name: "queue independent ordered entries offline",
            run: async () => {
              const album = await owner
                .insert(app.albums, { title: "Offline", artist: "Jazz" })
                .wait({ tier: "local" });
              const ownerTrack = await owner
                .insert(app.tracks, {
                  album_id: album.id,
                  title: "Owner",
                  ordinal: 1,
                  duration_ms: 1,
                })
                .wait({ tier: "local" });
              const editorTrack = await editor
                .insert(app.tracks, {
                  album_id: album.id,
                  title: "Editor",
                  ordinal: 2,
                  duration_ms: 1,
                })
                .wait({ tier: "local" });
              await Promise.all([
                owner
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: ownerTrack.id,
                    position: 30,
                  })
                  .wait({ tier: "local" }),
                editor
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: editorTrack.id,
                    position: 31,
                  })
                  .wait({ tier: "local" }),
              ]);
            },
            faultsAfter: [
              { kind: "reconnect", target: "owner" },
              { kind: "reconnect", target: "editor" },
            ],
          },
          {
            name: "converge, then owner revokes editor",
            run: async () => {
              const expectedPositions = [
                ...Array.from(
                  { length: PLAYLIST_WINDOW_OFFSET + PLAYLIST_WINDOW_LIMIT + 1 },
                  (_, position) => position,
                ),
                30,
                31,
              ];
              const expected = (rows: Array<{ position: number }>) =>
                rows.length === expectedPositions.length &&
                rows.map((row) => row.position).join(",") === expectedPositions.join(",");
              await Promise.all([
                waitForQuery(
                  owner,
                  app.playlist_entries
                    .where({ playlist_id: playlist.id })
                    .orderBy("position", "asc"),
                  expected,
                  "owner exact convergence",
                  20_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.playlist_entries
                    .where({ playlist_id: playlist.id })
                    .orderBy("position", "asc"),
                  expected,
                  "editor exact convergence",
                  20_000,
                  "edge",
                ),
              ]);
              await owner.delete(app.invitations, editorInvite.id).wait({ tier: "edge" });
              await Promise.all([
                waitForQuery(
                  editor,
                  app.playlist_entries
                    .where({ playlist_id: playlist.id })
                    .orderBy("position", "asc")
                    .offset(PLAYLIST_WINDOW_OFFSET)
                    .limit(PLAYLIST_WINDOW_LIMIT),
                  (rows) => rows.length === 0,
                  "revoked editor loses rendered playlist window",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.playlist_entries.where({ id: belowWindowEntryId }),
                  (rows) => rows.length === 0,
                  "revoked editor loses a child below the rendered window",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.playlists.where({ id: playlist.id }),
                  (rows) => rows.length === 0,
                  "revoked editor loses the playlist root",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.invitations.where({ id: editorInvite.id }),
                  (rows) => rows.length === 0,
                  "revoked editor loses the revoked invitation",
                  15_000,
                  "edge",
                ),
              ]);
              await expect(
                editor
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: "00000000-0000-0000-0000-000000000000",
                    position: 3,
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
            },
          },
          {
            name: "reopen persistent editor storage without restoring revoked access",
            run: async () => {
              // Reopening a second Db instance over the exact persistent name
              // exercises IndexedDB recovery rather than a fresh in-memory view.
              ctx.untrack(editor);
              await editor.shutdown();
              editor = await openClient(server, "editor-reopened", editorToken, editorDbName);
              await Promise.all([
                waitForQuery(
                  editor,
                  app.playlist_entries
                    .where({ playlist_id: playlist.id })
                    .orderBy("position", "asc")
                    .offset(PLAYLIST_WINDOW_OFFSET)
                    .limit(PLAYLIST_WINDOW_LIMIT),
                  (rows) => rows.length === 0,
                  "persistent reopen retains playlist-entry revocation",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.playlist_entries.where({ id: belowWindowEntryId }),
                  (rows) => rows.length === 0,
                  "persistent reopen retains below-window child revocation",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.playlists.where({ id: playlist.id }),
                  (rows) => rows.length === 0,
                  "persistent reopen retains root-row revocation",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  editor,
                  app.invitations.where({ id: editorInvite.id }),
                  (rows) => rows.length === 0,
                  "persistent reopen retains invitation revocation",
                  15_000,
                  "edge",
                ),
              ]);
            },
          },
        ],
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
  }, 120_000);
});

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`record-player-${label}`),
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName },
    }),
  );
}

/**
 * External authentication retains its own raw `sub`; Jazz authorization uses
 * the canonical issuer-scoped session user derived from that JWT. Invitations store
 * precisely that value, so a same-`sub` token from another issuer cannot read
 * or accept a grant.
 */
function canonicalUser(token: string): string {
  const claims = JSON.parse(atob(token.split(".")[1]!)) as { iss: string; sub: string };
  return userIdentity(claims.iss, claims.sub);
}

function audioStream(chunks: readonly Uint8Array[]): ReadableStream<Uint8Array> {
  let next = 0;
  return new ReadableStream({
    async pull(controller) {
      await delay(0);
      const chunk = chunks[next];
      if (chunk) {
        controller.enqueue(chunk);
        next += 1;
      } else {
        controller.close();
      }
    },
  });
}

function failingAudioStream(): ReadableStream<Uint8Array> {
  let yielded = false;
  return new ReadableStream({
    async pull(controller) {
      await delay(0);
      if (!yielded) {
        yielded = true;
        controller.enqueue(new Uint8Array(32 * 1024).fill(0x70));
        return;
      }
      controller.error(new Error("record-player injected audio source failure"));
    },
  });
}

function deterministicAudioChunks(): Uint8Array[] {
  return Array.from({ length: 4 }, (_, chunkIndex) =>
    Uint8Array.from(
      { length: 32 * 1024 },
      (_, byteIndex) => (chunkIndex * 67 + byteIndex * 31) % 251,
    ),
  );
}

function concatenateBytes(chunks: readonly Uint8Array[]): Uint8Array {
  const bytes = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

async function delay(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}
