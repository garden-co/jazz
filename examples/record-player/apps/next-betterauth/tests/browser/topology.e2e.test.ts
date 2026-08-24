import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  TestCleanup,
  uniqueDbName,
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

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

describe("RecordPlayer authenticated playlist topology", () => {
  it("rejects forged acceptance and converges two offline playlist editors exactly", async () => {
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let listener: Db;
    let playlist: { id: string };
    let editorInvite: { id: string };
    let listenerInvite: { id: string };
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
              const [ownerToken, editorToken, listenerToken] = await Promise.all([
                getJazzServerJwtForUser("record-player-owner", undefined, server.appId),
                getJazzServerJwtForUser("record-player-editor", undefined, server.appId),
                getJazzServerJwtForUser("record-player-listener", undefined, server.appId),
              ]);
              console.info("[record-player-topology] open owner edge");
              owner = await openClient(server, "owner", ownerToken);
              console.info("[record-player-topology] open editor edge");
              editor = await openClient(server, "editor", editorToken);
              console.info("[record-player-topology] open listener edge");
              listener = await openClient(server, "listener", listenerToken);
              console.info("[record-player-topology] create playlist");
              playlist = (
                await owner
                  .insert(app.playlists, {
                    name: "Road tape",
                    owner_subject: "record-player-owner",
                  })
                  .wait({ tier: "edge" })
              ).value;
              editorInvite = (
                await owner
                  .insert(app.invitations, {
                    playlist_id: playlist.id,
                    subject: "record-player-editor",
                    role: "editor",
                    status: "pending",
                  })
                  .wait({ tier: "edge" })
              ).value;
              listenerInvite = (
                await owner
                  .insert(app.invitations, {
                    playlist_id: playlist.id,
                    subject: "record-player-listener",
                    role: "listener",
                    status: "pending",
                  })
                  .wait({ tier: "edge" })
              ).value;
              await Promise.all([
                editor
                  .update(app.invitations, editorInvite.id, { status: "accepted" })
                  .wait({ tier: "edge" }),
                listener
                  .update(app.invitations, listenerInvite.id, { status: "accepted" })
                  .wait({ tier: "edge" }),
              ]);
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
              const track = (
                await owner
                  .insert(app.tracks, {
                    album_id: (
                      await owner
                        .insert(app.albums, { title: "Receipt", artist: "Jazz" })
                        .wait({ tier: "edge" })
                    ).value.id,
                    title: "Boundary",
                    ordinal: 0,
                    duration_ms: 1,
                  })
                  .wait({ tier: "edge" })
              ).value;
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
              const album = (
                await owner
                  .insert(app.albums, { title: "Offline", artist: "Jazz" })
                  .wait({ tier: "local" })
              ).value;
              const ownerTrack = (
                await owner
                  .insert(app.tracks, {
                    album_id: album.id,
                    title: "Owner",
                    ordinal: 1,
                    duration_ms: 1,
                  })
                  .wait({ tier: "local" })
              ).value;
              const editorTrack = (
                await editor
                  .insert(app.tracks, {
                    album_id: album.id,
                    title: "Editor",
                    ordinal: 2,
                    duration_ms: 1,
                  })
                  .wait({ tier: "local" })
              ).value;
              await Promise.all([
                owner
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: ownerTrack.id,
                    position: 2,
                  })
                  .wait({ tier: "local" }),
                editor
                  .insert(app.playlist_entries, {
                    playlist_id: playlist.id,
                    track_id: editorTrack.id,
                    position: 1,
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
                rows.length === 2 && rows.map((row) => row.position).join(",") === "1,2";
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
        ],
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
  }, 90_000);
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
      driver: { type: "persistent", dbName: uniqueDbName(`record-player-${label}`) },
    }),
  );
}
