import { afterEach, describe, expect, it } from "vitest";
import { createDb, schema as s, type Db } from "jazz-tools";
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
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { PLAYLIST_WINDOW_LIMIT, PLAYLIST_WINDOW_OFFSET } from "../../src/record-player.js";

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
// an application-owned scalar subject rather than by `$createdBy`.
const recipientApp = s.defineApp({
  invitations: s.table({ subject: s.string(), label: s.string() }),
});
const recipientPermissions = s.definePermissions(recipientApp, ({ policy, session, anyOf }) => {
  policy.invitations.allowRead.where(
    anyOf([{ subject: session.user_id }, { label: "unmatched control branch" }]),
  );
  policy.invitations.allowInsert.always();
});

// This adds exactly the branch which distinguishes RecordPlayer from the
// scalar control: a correlated owner path through a referenced playlist.
const relationalRecipientApp = s.defineApp({
  albums: s.table({ title: s.string() }),
  tracks: s.table({ album_id: s.ref("albums"), title: s.string() }),
  playlists: s.table({ name: s.string(), owner_subject: s.string() }),
  invitations: s.table({
    playlist_id: s.ref("playlists"),
    subject: s.string(),
    label: s.string(),
    role: s.enum("listener", "editor"),
    status: s.enum("pending", "accepted"),
  }),
  playlist_entries: s.table({
    playlist_id: s.ref("playlists"),
    track_id: s.ref("tracks"),
    label: s.string(),
  }),
  playback_positions: s.table({
    playlist_id: s.ref("playlists"),
    track_id: s.ref("tracks"),
    position_ms: s.int(),
  }),
});
const relationalRecipientPermissions = s.definePermissions(
  relationalRecipientApp,
  ({ policy, session, anyOf, allowedTo }) => {
    policy.albums.allowRead.where({});
    policy.albums.allowInsert.always();
    policy.tracks.allowRead.where({});
    policy.tracks.allowInsert.always();
    const canEditPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.author },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          subject: session.user_id,
          role: "editor",
          status: "accepted",
        }),
      ]);
    policy.playlists.allowRead.where((playlist) =>
      anyOf([
        { $createdBy: session.author },
        policy.invitations.exists.where({
          playlist_id: playlist.id,
          subject: session.user_id,
          status: "accepted",
        }),
      ]),
    );
    policy.playlists.allowInsert.always();
    policy.playlists.allowUpdate.where({ $createdBy: session.author });
    policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
    policy.playlist_entries.allowInsert.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowUpdate.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowDelete.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playback_positions.allowRead.where({ $createdBy: session.author });
    policy.playback_positions.allowInsert.always();
    policy.invitations.allowRead.where((invite) =>
      anyOf([
        { subject: session.user_id },
        policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
      ]),
    );
    policy.invitations.allowInsert.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
    );
    policy.invitations.allowUpdate.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
    );
    policy.invitations.allowUpdate
      .whereOld({ subject: session.user_id, status: "pending" })
      .whereNew((invite) =>
        policy.invitations.exists.where({
          id: invite.id,
          playlist_id: invite.playlist_id,
          subject: invite.subject,
          role: invite.role,
          status: "pending",
        }),
      )
      .whereNew({ subject: session.user_id, status: "accepted" });
    policy.invitations.allowDelete.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
    );
  },
);

describe("RecordPlayer authenticated playlist topology", () => {
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
    const owner = await openClient(server, "scalar-owner", ownerToken);
    const recipient = await openClient(server, "scalar-recipient", recipientToken);
    const invite = await owner
      .insert(recipientApp.invitations, {
        subject: "record-player-scalar-recipient",
        label: "recipient routing receipt",
      })
      .wait({ tier: "edge" });

    await expect(
      waitForQuery(
        recipient,
        recipientApp.invitations.where({ subject: "record-player-scalar-recipient" }),
        (rows) => rows.length === 1 && rows[0]?.id === invite.id,
        "external-JWT scalar recipient receives invitation",
        15_000,
        "edge",
      ),
    ).resolves.toEqual([
      {
        id: invite.id,
        subject: "record-player-scalar-recipient",
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
    const owner = await openClient(server, "relation-owner", ownerToken);
    const recipient = await openClient(server, "relation-recipient", recipientToken);
    const secondRecipient = await openClient(server, "relation-listener", secondRecipientToken);
    const playlist = await owner
      .insert(relationalRecipientApp.playlists, {
        name: "recipient relation receipt",
        owner_subject: "record-player-relation-owner",
      })
      .wait({ tier: "edge" });
    const invite = await owner
      .insert(relationalRecipientApp.invitations, {
        playlist_id: playlist.id,
        subject: "record-player-relation-recipient",
        label: "recipient relation receipt",
        role: "listener",
        status: "pending",
      })
      .wait({ tier: "edge" });
    const secondInvite = await owner
      .insert(relationalRecipientApp.invitations, {
        playlist_id: playlist.id,
        subject: "record-player-relation-listener",
        label: "second recipient relation receipt",
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
                      subject: "record-player-relation-recipient",
                    }),
                    (rows) => rows.length === 1 && rows[0]?.id === invite.id,
                    "external-JWT recipient receives scalar grant with correlated owner branch",
                    15_000,
                    "edge",
                  ),
                  waitForQuery(
                    secondRecipient,
                    relationalRecipientApp.invitations.where({
                      subject: "record-player-relation-listener",
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
              const [ownerToken, recipientToken] = await Promise.all([
                getJazzServerJwtForUser("record-player-phase-owner", undefined, server.appId),
                getJazzServerJwtForUser("record-player-phase-recipient", undefined, server.appId),
              ]);
              const owner = await openClient(server, "phase-owner", ownerToken);
              const recipient = await openClient(server, "phase-recipient", recipientToken);
              const playlist = await owner
                .insert(relationalRecipientApp.playlists, {
                  name: "phase relation receipt",
                  owner_subject: "record-player-phase-owner",
                })
                .wait({ tier: "edge" });
              const invite = await owner
                .insert(relationalRecipientApp.invitations, {
                  playlist_id: playlist.id,
                  subject: "record-player-phase-recipient",
                  label: "phase relation receipt",
                  role: "listener",
                  status: "pending",
                })
                .wait({ tier: "edge" });
              await waitForQuery(
                recipient,
                relationalRecipientApp.invitations.where({
                  subject: "record-player-phase-recipient",
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
    let editorDbName: string;
    let streamedTrackId: string;
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
              editorDbName = uniqueDbName("record-player-editor-persistent");
              console.info("[record-player-topology] open owner edge");
              owner = await openClient(server, "owner", ownerToken);
              console.info("[record-player-topology] open editor edge");
              editor = await openClient(server, "editor", editorToken, editorDbName);
              console.info("[record-player-topology] open listener edge");
              listener = await openClient(server, "listener", listenerToken);
              console.info("[record-player-topology] create playlist");
              const mutationErrors: unknown[] = [];
              const stopMutationErrors = owner.onMutationError((event) =>
                mutationErrors.push(event),
              );
              const playlistWrite = owner.insert(app.playlists, {
                name: "Road tape",
                owner_subject: "record-player-owner",
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
                const settledPlaylist = await withTimeout(
                  playlistWrite.wait({ tier: "edge" }),
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
              editorInvite = await owner
                .insert(app.invitations, {
                  playlist_id: playlist.id,
                  subject: "record-player-editor",
                  role: "editor",
                  status: "pending",
                })
                .wait({ tier: "edge" });
              listenerInvite = await owner
                .insert(app.invitations, {
                  playlist_id: playlist.id,
                  subject: "record-player-listener",
                  role: "listener",
                  status: "pending",
                })
                .wait({ tier: "edge" });
              // Edge settlement acknowledges the owner's write; it does not
              // imply either recipient has received the row yet. A recipient
              // accepts an invitation only after its normal read path observes
              // the pending row.
              await Promise.all([
                waitForQuery(
                  editor,
                  app.invitations.where({ subject: "record-player-editor" }),
                  (rows) => rows[0]?.id === editorInvite.id && rows[0]?.status === "pending",
                  "editor observes pending invitation",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  listener,
                  app.invitations.where({ subject: "record-player-listener" }),
                  (rows) => rows[0]?.id === listenerInvite.id && rows[0]?.status === "pending",
                  "listener observes pending invitation",
                  15_000,
                  "edge",
                ),
              ]);
              await Promise.all([
                editor
                  .update(app.invitations, editorInvite.id, { status: "accepted" })
                  .wait({ tier: "edge" }),
                listener
                  .update(app.invitations, listenerInvite.id, { status: "accepted" })
                  .wait({ tier: "edge" }),
              ]);

              // This uses precisely the public streaming API that the app's
              // persistence adapter uses. The list screens must remain useful
              // without eagerly materializing the audio field.
              const streamedTrack = await owner.insertStreaming(app.tracks, {
                album_id: (
                  await owner
                    .insert(app.albums, { title: "A streamed record", artist: "Jazz" })
                    .wait({ tier: "edge" })
                ).id,
                title: "Streaming receipt",
                ordinal: 0,
                duration_ms: 2,
                audio_bytes: audioStream([0x52, 0x50, 0x2d, 0x31]),
              });
              streamedTrackId = (
                await withTimeout(
                  streamedTrack.wait({ tier: "edge" }),
                  15_000,
                  "streamed audio track did not settle at edge",
                )
              ).id;
            },
          },
          {
            name: "exercise metadata and window queries used by the rendered screens",
            run: async () => {
              const windowAlbum = await owner
                .insert(app.albums, { title: "Window catalogue", artist: "Jazz" })
                .wait({ tier: "edge" });
              const windowTracks: Array<{ id: string }> = [];
              for (let position = 0; position < PLAYLIST_WINDOW_OFFSET + 2; position += 1) {
                const track = await owner
                  .insert(app.tracks, {
                    album_id: windowAlbum.id,
                    title: `Window ${position}`,
                    ordinal: position,
                    duration_ms: position + 1,
                  })
                  .wait({ tier: "edge" });
                await owner
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: track.id,
                    position,
                  })
                  .wait({ tier: "edge" });
                windowTracks.push(track);
              }

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
              // deliberately nonzero window offset. Verify the offset rather than
              // merely an unbounded equivalent query.
              const visibleWindow = await waitForQuery(
                listener,
                app.playlist_entries
                  .where({ playlist_id: playlist.id })
                  .orderBy("position", "asc")
                  .offset(PLAYLIST_WINDOW_OFFSET)
                  .limit(PLAYLIST_WINDOW_LIMIT),
                (rows) =>
                  rows.length === 2 &&
                  rows.map((row) => row.position).join(",") ===
                    `${PLAYLIST_WINDOW_OFFSET},${PLAYLIST_WINDOW_OFFSET + 1}`,
                "listener rendered playlist window",
                15_000,
                "edge",
              );
              expect(visibleWindow.map((row) => row.track_id)).toEqual(
                windowTracks.slice(PLAYLIST_WINDOW_OFFSET).map((track) => track.id),
              );

              // A metadata-only projection proves the streamed byte field does
              // not force an eager payload read merely because it belongs to a
              // visible catalogue row.
              await waitForQuery(
                listener,
                app.tracks
                  .where({ id: streamedTrackId })
                  .select("id", "album_id", "title", "ordinal", "duration_ms"),
                (rows) => rows[0]?.id === streamedTrackId && rows[0]?.title === "Streaming receipt",
                "streamed track metadata without audio materialization",
                15_000,
                "edge",
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
                    position: 20,
                  })
                  .wait({ tier: "local" }),
                editor
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: editorTrack.id,
                    position: 21,
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
              const expected = (rows: Array<{ position: number }>) =>
                rows.length === PLAYLIST_WINDOW_OFFSET + 4 &&
                rows.map((row) => row.position).join(",") === "0,1,2,3,4,5,6,7,8,9,20,21";
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
              await waitForQuery(
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
              );
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
              await waitForQuery(
                editor,
                app.playlist_entries
                  .where({ playlist_id: playlist.id })
                  .orderBy("position", "asc")
                  .offset(PLAYLIST_WINDOW_OFFSET)
                  .limit(PLAYLIST_WINDOW_LIMIT),
                (rows) => rows.length === 0,
                "persistent reopen retains revocation",
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

function audioStream(bytes: readonly number[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      // Deliberately split the payload so this test covers stream consumption,
      // not only a one-chunk convenience path.
      controller.enqueue(new Uint8Array(bytes.slice(0, 2)));
      controller.enqueue(new Uint8Array(bytes.slice(2)));
      controller.close();
    },
  });
}
