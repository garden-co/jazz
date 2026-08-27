import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../../../../../packages/jazz-tools/src/runtime/db.js";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  sleep,
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
} from "../../../../../../packages/jazz-tools/tests/browser/support.js";
import {
  browserTopologyReporter,
  runTopologyScenario,
  TopologyEnvelopeScheduler,
} from "../../../../../../packages/jazz-tools/tests/browser/topology-harness.js";
import {
  blockJazzServerNetwork,
  getJazzServerInfo,
  getJazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server.js";
import permissions from "../../permissions.js";
import { app, type Step } from "../../schema.js";

const ctx = new TestCleanup();
let pendingServerUnblock: string | undefined;

async function restoreNetworkAndCleanup(): Promise<void> {
  let networkError: unknown;
  if (pendingServerUnblock) {
    try {
      await unblockJazzServerNetwork(pendingServerUnblock);
      pendingServerUnblock = undefined;
    } catch (error) {
      networkError = error;
    }
  }
  await ctx.cleanup();
  if (networkError) {
    throw new Error("failed to restore Wequencer test network", { cause: networkError });
  }
}

afterEach(restoreNetworkAndCleanup);

const trackNames = ["Kick", "Snare", "Closed hat", "Bass"];
const stepsPerTrack = 16;
const PHASE_TIMEOUT_MS = 25_000;
const FAULT_TIMEOUT_MS = 15_000;
const CLEANUP_TIMEOUT_MS = 10_000;
const PHASE_COUNT = 6;
const FAULT_COUNT = 5;
const COMPENSATION_COUNT = 1;
const SCHEDULER_COUNT = 1;
const OUTER_BUFFER_MS = 15_000;
const OUTER_TIMEOUT_MS =
  PHASE_COUNT * PHASE_TIMEOUT_MS +
  FAULT_COUNT * FAULT_TIMEOUT_MS +
  COMPENSATION_COUNT * CLEANUP_TIMEOUT_MS +
  CLEANUP_TIMEOUT_MS +
  SCHEDULER_COUNT * FAULT_TIMEOUT_MS +
  OUTER_BUFFER_MS;
const TOPOLOGY_TEST_NAME =
  "converges sequencer edits, retryable transport, reconnect, reopen, and revocation";
declare const __JAZZ_EXAMPLE_TOPOLOGY_SEED__: string;

function topologyTest(name: string, run: () => Promise<void>): void {
  it(name, run, OUTER_TIMEOUT_MS);
}

/**
 * Exercises the actual ordered queries rendered by SequencerSession and
 * TrackLane across browser -> edge -> core. Transport observations are normal
 * convergent rows, so this proves no wall-clock or sample-accurate guarantee.
 */
describe("Wequencer cross-topology recovery", () => {
  topologyTest(TOPOLOGY_TEST_NAME, async () => {
    const requestedSeed = Number(__JAZZ_EXAMPLE_TOPOLOGY_SEED__);
    const seed = Number.isSafeInteger(requestedSeed) ? requestedSeed : 61;
    const scheduler = new TopologyEnvelopeScheduler(seed);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let ownerToken: string;
    let ownerDbName: string;
    let editorToken: string;
    let editorDbName: string;
    let session: { id: string };
    let ownerProfile: { id: string };
    let editorProfile: { id: string };
    let ownerPresence: { id: string };
    let editorPresence: { id: string };
    let editorMembership: { id: string };
    let tracks: Array<{ id: string }>;
    let offlineStep: { id: string };
    let subscribedOwnerStepId: string;
    let transport: { id: string } | undefined;
    let subscribedTrackSteps: Step[] = [];

    const receipt = await runTopologyScenario(
      {
        id: "wequencer-browser-edge-core-recovery",
        topology: ["browser", "edge", "core"],
        seed,
        phaseTimeoutMs: PHASE_TIMEOUT_MS,
        faultTimeoutMs: FAULT_TIMEOUT_MS,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/wequencer/apps/next-betterauth test:browser:focused -- tests/browser/topology.e2e.test.ts`,
        envelopeSchedulers: [scheduler],
        targets: {
          owner: {
            disconnect: async () => owner.disconnect(),
            reconnect: async () => owner.reconnect(),
            restart: async () => {
              await owner.shutdown();
              ctx.untrack(owner);
              owner = await openClient(server, "owner", ownerToken, ownerDbName);
            },
          },
          editor: {
            restart: async () => {
              // Prevent the replacement client from cold-refetching before
              // the next phase can prove what survived in IndexedDB.
              pendingServerUnblock = server.serverUrl;
              await blockJazzServerNetwork(server.serverUrl);
              await editor.disconnect();
              await editor.shutdown();
              ctx.untrack(editor);
              editor = await openClient(server, "editor", editorToken, editorDbName);
            },
          },
          authorization: {
            failure: async () => {
              const outsider = await openClient(
                server,
                "outsider",
                await getJazzServerJwtForUser("wequencer-outsider", undefined, server.appId),
              );
              await expect(
                outsider
                  .insert(app.tracks, {
                    session_id: session.id,
                    position: 99,
                    name: "unauthorized",
                    color: "#000000",
                  })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        },
        phases: [
          {
            name: "owner bootstrap and editor admission",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("wequencer-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [issuedOwnerToken, issuedEditorToken] = await Promise.all([
                getJazzServerJwtForUser("wequencer-owner", undefined, server.appId),
                getJazzServerJwtForUser("wequencer-editor", undefined, server.appId),
              ]);
              ownerToken = issuedOwnerToken;
              ownerDbName = uniqueDbName("wequencer-owner");
              editorToken = issuedEditorToken;
              editorDbName = uniqueDbName("wequencer-editor");
              owner = await openClient(server, "owner", ownerToken, ownerDbName);
              editor = await openClient(server, "editor", editorToken, editorDbName);
              ownerProfile = await owner
                .insert(app.profiles, { user_id: "wequencer-owner", display_name: "Owner" })
                .wait({ tier: "edge" });
              editorProfile = await editor
                .insert(app.profiles, { user_id: "wequencer-editor", display_name: "Editor" })
                .wait({ tier: "edge" });
              session = await owner
                .insert(app.sessions, {
                  title: "Topology rehearsal",
                  tempo_bpm: 124,
                  loop_steps: 16,
                })
                .wait({ tier: "edge" });
              await owner
                .insert(app.session_members, {
                  session_id: session.id,
                  user_id: "wequencer-owner",
                  role: "owner",
                })
                .wait({ tier: "edge" });
              editorMembership = await owner
                .insert(app.session_members, {
                  session_id: session.id,
                  user_id: "wequencer-editor",
                  role: "editor",
                })
                .wait({ tier: "edge" });
              tracks = await Promise.all(
                trackNames.map(
                  async (name, position) =>
                    await owner
                      .insert(app.tracks, {
                        session_id: session.id,
                        position,
                        name,
                        color: `#${position}${position}${position}`,
                      })
                      .wait({ tier: "edge" }),
                ),
              );
              await Promise.all(
                tracks.flatMap((track) =>
                  Array.from({ length: stepsPerTrack }, (_, position) =>
                    owner
                      .insert(app.steps, {
                        track_id: track.id,
                        position,
                        enabled: false,
                        velocity: 100,
                        probability: 100,
                      })
                      .wait({ tier: "edge" }),
                  ),
                ),
              );
              await waitForQuery(
                editor,
                sessionQueries(session.id).tracks,
                (rows) => rows.length === trackNames.length,
                "editor receives session tracks",
                15_000,
                "edge",
              );
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "concurrent ordered sequencer edits and presence",
            run: async () => {
              const [ownerSteps, editorSteps] = await Promise.all([
                owner.all(trackSteps(tracks[0].id), { tier: "edge" }),
                editor.all(trackSteps(tracks[1].id), { tier: "edge" }),
              ]);
              const ownerStepId = ownerSteps[1]!.id;
              subscribedOwnerStepId = ownerStepId;
              // Subscribe before either writer commits. This is the exact
              // ordered TrackLane query rendered by a collaborator. Reduce
              // its stream exactly as a consumer does, so this receipt proves
              // both its pre-write false snapshot and the later remote update.
              ctx.trackSubscription(
                editor.subscribe(trackSteps(tracks[0].id), (rows) => {
                  subscribedTrackSteps = rows;
                }),
              );
              await waitForCondition(
                () =>
                  Promise.resolve(
                    subscribedTrackSteps.some(
                      (step) => step.id === ownerStepId && step.enabled === false,
                    ),
                  ),
                15_000,
                "TrackLane subscription receives the pre-write owner step",
              );
              await Promise.all([
                owner.update(app.steps, ownerStepId, { enabled: true }).wait({ tier: "edge" }),
                editor
                  .update(app.steps, editorSteps[2].id, { enabled: true })
                  .wait({ tier: "edge" }),
                owner
                  .insert(app.presence, {
                    session_id: session.id,
                    profile_id: ownerProfile.id,
                    cursor_step: 1,
                    heartbeat_at: new Date(),
                  })
                  .wait({ tier: "edge" })
                  .then((row) => (ownerPresence = row)),
                editor
                  .insert(app.presence, {
                    session_id: session.id,
                    profile_id: editorProfile.id,
                    cursor_step: 2,
                    heartbeat_at: new Date(),
                  })
                  .wait({ tier: "edge" })
                  .then((row) => (editorPresence = row)),
              ]);
              const ownerTrackSteps = await waitForQuery(
                editor,
                trackSteps(tracks[0].id),
                (rows) => rows.length === stepsPerTrack && rows[1]?.enabled === true,
                "editor receives owner's ordered step edit",
                15_000,
                "edge",
              );
              expect(ownerTrackSteps.map((step) => step.position)).toEqual(
                Array.from({ length: stepsPerTrack }, (_, position) => position),
              );
              await waitForCondition(
                () =>
                  Promise.resolve(
                    subscribedTrackSteps.some(
                      (step) => step.id === ownerStepId && step.enabled === true,
                    ),
                  ),
                15_000,
                "TrackLane subscription receives the owner's post-write update",
              );
              await waitForQuery(
                owner,
                trackSteps(tracks[1].id),
                (rows) => rows.length === stepsPerTrack && rows[2]?.enabled === true,
                "owner receives editor's ordered step edit",
                15_000,
                "edge",
              );
              const presence = await waitForQuery(
                editor,
                sessionQueries(session.id).presence,
                (rows) => rows.some((row) => row.id === ownerPresence.id && row.cursor_step === 1),
                "editor receives concurrent owner presence",
                15_000,
                "edge",
              );
              expect(presence.find((row) => row.id === editorPresence.id)).toMatchObject({
                cursor_step: 2,
              });
            },
            faultsAfter: [{ kind: "disconnect", target: "owner" }],
          },
          {
            name: "offline local edit and deterministic transport retry",
            run: async () => {
              const ownerSteps = await owner.all(trackSteps(tracks[2].id), { tier: "local" });
              offlineStep = { id: ownerSteps[3].id };
              await owner
                .update(app.steps, offlineStep.id, { enabled: true })
                .wait({ tier: "local" });
              expect(
                (await owner.all(trackSteps(tracks[2].id), { tier: "local" })).find(
                  (step) => step.id === offlineStep.id,
                ),
              ).toMatchObject({ enabled: true, position: 3 });

              // Repeated edge reads on the still-connected editor prove that
              // the owner's optimistic edit stays private for the duration
              // of the partition, rather than merely losing a race once.
              for (let attempt = 0; attempt < 3; attempt += 1) {
                const peerSteps = await editor.all(trackSteps(tracks[2].id), { tier: "edge" });
                expect(peerSteps.find((step) => step.id === offlineStep.id)).toMatchObject({
                  enabled: false,
                  position: 3,
                });
                await sleep(150);
              }

              // Test-owned operation scheduling only: the runtime transport is
              // untouched. A retried transport receipt remains one ordinary row.
              scheduler.dropNextThenRetry(1);
              await scheduler.intercept(
                { from: "editor", to: "edge", label: "transport-observation" },
                undefined,
                async (_value, context) => {
                  transport = await editor
                    .insert(app.transport_observations, {
                      session_id: session.id,
                      playing: true,
                      bar: context.attempt,
                      observed_at: new Date(),
                    })
                    .wait({ tier: "edge" });
                },
              );
              expect(transport).toBeUndefined();
              await scheduler.advance(1);
              expect(transport).toBeDefined();
            },
            faultsAfter: [
              { kind: "reconnect", target: "owner" },
              { kind: "restart", target: "owner" },
            ],
          },
          {
            name: "persistent reopen and peer convergence",
            run: async () => {
              const ownerTracks = await waitForQuery(
                owner,
                sessionQueries(session.id).tracks,
                (rows) => rows.length === trackNames.length,
                "persistent owner reopens session tracks",
                20_000,
                "edge",
              );
              expect(ownerTracks.map((row) => row.position)).toEqual([0, 1, 2, 3]);
              const replayedSteps = await waitForQuery(
                editor,
                trackSteps(tracks[2].id),
                (rows) => rows.some((step) => step.id === offlineStep.id && step.enabled),
                "editor receives owner offline step",
                20_000,
                "edge",
              );
              expect(replayedSteps).toHaveLength(stepsPerTrack);
              expect(subscribedTrackSteps).toHaveLength(stepsPerTrack);
              // The collaborator's subscription was established before the
              // owner's partition. A later update after reconnect *and*
              // owner restart must still reach that same subscription rather
              // than merely being visible to a fresh one-shot read.
              await owner
                .update(app.steps, subscribedOwnerStepId, { enabled: false })
                .wait({ tier: "edge" });
              await waitForCondition(
                () =>
                  Promise.resolve(
                    subscribedTrackSteps.some(
                      (step) => step.id === subscribedOwnerStepId && step.enabled === false,
                    ),
                  ),
                20_000,
                "existing TrackLane subscription receives owner update after recovery",
              );
              expect(
                await editor.all(sessionQueries(session.id).presence, { tier: "edge" }),
              ).toHaveLength(2);
              // This directly exercises the durable row/sync contract used by
              // the app heartbeat. Its cadence and timer cleanup have a
              // separate deterministic unit receipt; this proves that a
              // post-reopen heartbeat is an update, not a growing trail of
              // presence rows.
              await owner
                .update(app.presence, ownerPresence.id, {
                  cursor_step: 7,
                  heartbeat_at: new Date(),
                })
                .wait({ tier: "edge" });
              const refreshedPresence = await waitForQuery(
                editor,
                sessionQueries(session.id).presence,
                (rows) => rows.some((row) => row.id === ownerPresence.id && row.cursor_step === 7),
                "editor receives reopened owner's heartbeat update",
                20_000,
                "edge",
              );
              expect(refreshedPresence).toHaveLength(2);
              const observations = await waitForQuery(
                editor,
                sessionQueries(session.id).observations,
                (rows) => rows.some((row) => row.id === transport!.id),
                "editor reads retried transport observation",
                20_000,
                "edge",
              );
              expect(observations.find((row) => row.id === transport!.id)).toMatchObject({
                playing: true,
                bar: 2,
              });
            },
            faultsAfter: [{ kind: "restart", target: "editor" }],
          },
          {
            name: "offline editor reopen rehydrates bounded projected session reads",
            run: async () => {
              const restoredTracks = await waitForQuery(
                editor,
                sessionQueries(session.id).tracks,
                (rows) => rows.length === trackNames.length,
                "offline persistent editor reopens session tracks from local storage",
                20_000,
                "local",
              );
              expect(restoredTracks.map((track) => track.position)).toEqual([0, 1, 2, 3]);

              // This is intentionally a separate read shape from the UI's
              // full track list: it proves that a constrained, projected
              // collaborator query remains bounded after durable reopen and
              // does not accidentally materialize unrelated track fields.
              const projectedWindow = await editor.all(
                sessionQueries(session.id).tracks.limit(2).select("id", "position", "name"),
                { tier: "local" },
              );
              expect(projectedWindow.map((track) => track.position)).toEqual([0, 1]);
              expect(projectedWindow).toHaveLength(2);
              expect("color" in projectedWindow[0]!).toBe(false);

              const restoredOfflineStep = await editor.all(trackSteps(tracks[2].id), {
                tier: "local",
              });
              expect(restoredOfflineStep.find((step) => step.id === offlineStep.id)).toMatchObject({
                enabled: true,
              });

              await unblockJazzServerNetwork(server.serverUrl);
              pendingServerUnblock = undefined;
              await editor.reconnect();
              const settledProjectedWindow = await waitForQuery(
                editor,
                sessionQueries(session.id).tracks.limit(2).select("id", "position", "name"),
                (rows) => rows.length === 2 && rows[0]?.position === 0 && rows[1]?.position === 1,
                "reconnected editor settles the exact projected track window at edge",
                20_000,
                "edge",
              );
              expect(settledProjectedWindow.map((track) => track.id)).toEqual(
                projectedWindow.map((track) => track.id),
              );
              expect(settledProjectedWindow.every((track) => !("color" in track))).toBe(true);
            },
          },
          {
            name: "membership revocation rejects former editor",
            run: async () => {
              await owner.delete(app.session_members, editorMembership.id).wait({ tier: "edge" });
              const editorSteps = await editor.all(trackSteps(tracks[1].id), { tier: "local" });
              await expect(
                editor
                  .update(app.steps, editorSteps[4].id, { enabled: true })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        ],
        cleanup: async () => ctx.cleanup(),
        cleanupTimeoutMs: CLEANUP_TIMEOUT_MS,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
      ["restart", "completed"],
      ["restart", "completed"],
    ]);
    expect(receipt.envelopes[0].activities.map((activity) => activity.action)).toContain("retried");
  });
});

function sessionQueries(sessionId: string) {
  return {
    tracks: app.tracks.where({ session_id: sessionId }).orderBy("position", "asc"),
    observations: app.transport_observations
      .where({ session_id: sessionId })
      .orderBy("observed_at", "desc"),
    presence: app.presence.where({ session_id: sessionId }),
  };
}

function trackSteps(trackId: string) {
  return app.steps.where({ track_id: trackId }).orderBy("position", "asc");
}

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`wequencer-${label}`),
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
