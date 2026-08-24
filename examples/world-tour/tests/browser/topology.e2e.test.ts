/**
 * Adopter-level sync receipt for the WorldTour itinerary query shape.
 *
 * This uses separate persistent browser clients, rather than component-local
 * state, so the receipt crosses the public client -> edge -> core -> peer path.
 */
import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../../../packages/jazz-tools/src/runtime/db.js";
import { deploy } from "../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  browserTopologyReporter,
  runTopologyScenario,
} from "../../../../packages/jazz-tools/tests/browser/topology-harness.js";
import {
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
} from "../../../../packages/jazz-tools/tests/browser/support.js";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../packages/jazz-tools/tests/browser/testing-server.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { assertWorldTourTopologyContract } from "./topology-contract.js";
import { TOPOLOGY_SEED } from "./topology-seed.js";

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

describe("WorldTour cross-topology itinerary recovery", () => {
  it("keeps the ordered venue window convergent across concurrent and offline edits", async () => {
    // Validate the complete runtime command adapter before this scenario can
    // create a client. This turns stale browser artifacts into a useful
    // immediate error rather than a mid-receipt timeout.
    assertWorldTourTopologyContract();
    const seed = TOPOLOGY_SEED;
    let server: Awaited<ReturnType<typeof getJazzServerInfo>> | undefined;
    let alice: Db | undefined;
    let bob: Db | undefined;
    let tour: { id: string } | undefined;
    let firstStop: { id: string } | undefined;
    let protectedStop: { id: string } | undefined;
    let offlineStop: { id: string } | undefined;
    let fallbackVenueId: string | undefined;
    let observedWindow: string[] = [];
    const aliceDbName = uniqueDbName("world-tour-alice");

    const receipt = await runTopologyScenario(
      {
        id: "world-tour.topology.itinerary-recovery",
        topology: ["browser", "edge", "core"],
        seed,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/world-tour test -- topology.e2e.test.ts`,
        targets: {
          alice: {
            disconnect: async () => alice!.disconnect(),
            reconnect: async () => alice!.reconnect(),
            restart: async () => {
              // A browser refresh must retain the same persisted namespace,
              // while creating a fresh client and connection.
              ctx.untrack(alice!);
              await alice!.shutdown();
              alice = await openClient(server!, "alice-reopened", "world-tour-alice", aliceDbName);
            },
          },
          authorization: {
            failure: async () => {
              const outsider = await openClient(server!, "outsider", "world-tour-outsider");
              await expect(
                outsider.one(app.stops.where({ id: firstStop!.id }), { tier: "edge" }),
              ).resolves.toMatchObject({ id: firstStop!.id, status: "confirmed" });
              // The third stop is tentative, hence visible to a band member
              // but not to a client which has no membership row.
              await expect(
                outsider.one(app.stops.where({ id: protectedStop!.id }), { tier: "edge" }),
              ).resolves.toBeNull();
              await expect(
                outsider
                  .update(app.stops, firstStop!.id, { status: "cancelled" })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        },
        phases: [
          {
            name: "edge bootstrap and itinerary query shape",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("world-tour-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              alice = await openClient(server, "alice", "world-tour-alice", aliceDbName);
              bob = await openClient(server, "bob", "world-tour-bob");

              const createdTour = await alice
                .insert(app.bands, { name: "Topology Tour" })
                .wait({ tier: "edge" });
              tour = createdTour;
              await Promise.all([
                alice
                  .insert(app.members, { bandId: createdTour.id, userId: "world-tour-alice" })
                  .wait({ tier: "edge" }),
                alice
                  .insert(app.members, { bandId: createdTour.id, userId: "world-tour-bob" })
                  .wait({ tier: "edge" }),
              ]);
              const [london, paris] = await Promise.all([
                alice
                  .insert(app.venues, {
                    name: "London Hall",
                    city: "London",
                    country: "UK",
                    lat: 51.5,
                    lng: -0.1,
                  })
                  .wait({ tier: "edge" }),
                alice
                  .insert(app.venues, {
                    name: "Paris Club",
                    city: "Paris",
                    country: "France",
                    lat: 48.9,
                    lng: 2.3,
                  })
                  .wait({ tier: "edge" }),
              ]);
              const [first, second, third] = await Promise.all([
                alice
                  .insert(app.stops, {
                    bandId: createdTour.id,
                    venueId: london.id,
                    date: new Date("2026-08-01"),
                    status: "confirmed",
                    publicDescription: "opening",
                  })
                  .wait({ tier: "edge" }),
                alice
                  .insert(app.stops, {
                    bandId: createdTour.id,
                    venueId: paris.id,
                    date: new Date("2026-08-03"),
                    status: "confirmed",
                    publicDescription: "second night",
                  })
                  .wait({ tier: "edge" }),
                alice
                  .insert(app.stops, {
                    bandId: createdTour.id,
                    venueId: paris.id,
                    date: new Date("2026-08-02"),
                    status: "tentative",
                    publicDescription: "members-only routing detail",
                  })
                  .wait({ tier: "edge" }),
              ]);
              // Keep another stop for this band just outside the date window.
              // It is deliberately earlier than the window: removing the date
              // predicate would otherwise still satisfy the bounded result's
              // expected IDs, despite no longer testing the app query.
              await alice
                .insert(app.stops, {
                  bandId: createdTour.id,
                  venueId: london.id,
                  date: new Date("2026-07-31"),
                  status: "confirmed",
                  publicDescription: "prior-leg archive",
                })
                .wait({ tier: "edge" });
              firstStop = first;
              protectedStop = third;
              fallbackVenueId = london.id;

              const itinerary = app.stops
                .where({
                  bandId: createdTour.id,
                  date: { gte: new Date("2026-08-01"), lte: new Date("2026-08-07") },
                })
                .include({ venue: true })
                .orderBy("date", "asc")
                .limit(2);
              const unsubscribe = bob.subscribeAll(itinerary, (snapshot) => {
                observedWindow = (snapshot.all ?? []).map(
                  (stop) => `${stop.date.toISOString()}:${stop.venue?.name}`,
                );
              });
              ctx.trackSubscription(unsubscribe);
              const rows = await waitForQuery(
                bob,
                itinerary,
                (value) => value.length === 2,
                "peer receives ordered itinerary window",
                15_000,
                "edge",
              );
              // Three matching stops exist; this verifies the app's actual
              // ordered, bounded query rather than an unbounded happy path.
              expect(rows.map((stop) => stop.id)).toEqual([first.id, third.id]);
              expect(rows.map((stop) => stop.venue?.name)).toEqual(["London Hall", "Paris Club"]);
              expect(second.id).not.toBe(third.id);
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "concurrent independent stop edits",
            run: async () => {
              await Promise.all([
                alice!
                  .update(app.stops, firstStop!.id, { status: "tentative" })
                  .wait({ tier: "local" }),
                bob!
                  .update(app.stops, firstStop!.id, { publicDescription: "peer-confirmed detail" })
                  .wait({ tier: "local" }),
              ]);
              await waitForCondition(
                async () => {
                  const [a, b] = await Promise.all([
                    alice!.one(app.stops.where({ id: firstStop!.id }), { tier: "edge" }),
                    bob!.one(app.stops.where({ id: firstStop!.id }), { tier: "edge" }),
                  ]);
                  return (
                    a?.status === "tentative" &&
                    a?.publicDescription === "peer-confirmed detail" &&
                    a?.status === b?.status &&
                    a?.publicDescription === b?.publicDescription
                  );
                },
                20_000,
                "concurrent per-field itinerary edits converge at edge",
              );
            },
            faultsAfter: [{ kind: "disconnect", target: "alice" }],
          },
          {
            name: "offline local itinerary insert stays local until reconnect",
            run: async () => {
              const insertedOffline = await alice!
                .insert(app.stops, {
                  bandId: tour!.id,
                  venueId: fallbackVenueId!,
                  // This lies between the first and third stops. It must
                  // displace the third stop from the bounded itinerary after
                  // the disconnected writer reconnects.
                  date: new Date("2026-08-01T12:00:00.000Z"),
                  status: "confirmed",
                  publicDescription: "offline routing note",
                })
                .wait({ tier: "local" });
              offlineStop = insertedOffline;
              const local = await alice!.one(app.stops.where({ id: offlineStop.id }), {
                tier: "local",
              });
              expect(local?.publicDescription).toBe("offline routing note");

              const itinerary = app.stops
                .where({
                  bandId: tour!.id,
                  date: { gte: new Date("2026-08-01"), lte: new Date("2026-08-07") },
                })
                .include({ venue: true })
                .orderBy("date", "asc")
                .limit(2);
              const localWindow = await waitForQuery(
                alice!,
                itinerary,
                (value) =>
                  value.length === 2 &&
                  value.map((stop) => stop.id).join(",") ===
                    [firstStop!.id, offlineStop!.id].join(","),
                "disconnected writer recomputes its bounded local itinerary",
                10_000,
                "local",
              );
              expect(localWindow.map((stop) => stop.id)).toEqual([firstStop!.id, offlineStop!.id]);
              expect(localWindow.map((stop) => stop.id)).not.toContain(protectedStop!.id);

              // The real disconnect is a topology disruption: Bob's edge
              // window must not learn Alice's local insertion before reconnect.
              const beforeReconnect = await bob!.all(itinerary, { tier: "edge" });
              expect(beforeReconnect.map((stop) => stop.id)).toEqual([
                firstStop!.id,
                protectedStop!.id,
              ]);
            },
            faultsAfter: [{ kind: "reconnect", target: "alice" }],
          },
          {
            name: "peer convergence and ordered subscription receipt",
            run: async () => {
              const itinerary = app.stops
                .where({
                  bandId: tour!.id,
                  date: { gte: new Date("2026-08-01"), lte: new Date("2026-08-07") },
                })
                .include({ venue: true })
                .orderBy("date", "asc")
                .limit(2);
              const rows = await waitForQuery(
                bob!,
                itinerary,
                (value) =>
                  value.length === 2 &&
                  value.map((stop) => stop.id).join(",") ===
                    [firstStop!.id, offlineStop!.id].join(","),
                "peer receives the offline insertion and recomputes its bounded itinerary after reconnect",
                20_000,
                "edge",
              );
              await waitForCondition(
                async () => observedWindow.length === 2,
                10_000,
                "ordered itinerary subscription converges",
              );
              expect(observedWindow).toEqual(
                rows.map((stop) => `${stop.date.toISOString()}:${stop.venue?.name}`),
              );
              expect(rows.map((stop) => stop.id)).toEqual([firstStop!.id, offlineStop!.id]);
              expect(rows.map((stop) => stop.id)).not.toContain(protectedStop!.id);
              expect(rows.map((stop) => stop.venue?.name)).toEqual(["London Hall", "London Hall"]);
            },
            faultsAfter: [{ kind: "restart", target: "alice" }],
          },
          {
            name: "persistent client restart retains the current local itinerary window",
            run: async () => {
              const itinerary = app.stops
                .where({
                  bandId: tour!.id,
                  date: { gte: new Date("2026-08-01"), lte: new Date("2026-08-07") },
                })
                .include({ venue: true })
                .orderBy("date", "asc")
                .limit(2);
              const rows = await waitForQuery(
                alice!,
                itinerary,
                (value) =>
                  value.length === 2 &&
                  value.map((stop) => stop.id).join(",") ===
                    [firstStop!.id, offlineStop!.id].join(","),
                "reopened client rehydrates its bounded itinerary from IndexedDB",
                15_000,
                "local",
              );
              expect(rows.map((stop) => stop.id)).toEqual([firstStop!.id, offlineStop!.id]);
              expect(rows.map((stop) => stop.venue?.name)).toEqual(["London Hall", "London Hall"]);
            },
          },
        ],
      },
      browserTopologyReporter,
    );

    expect(receipt).toMatchObject({ status: "passed", seed });
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
      ["restart", "completed"],
    ]);
  }, 90_000);
});

async function openClient(
  server: { appId: string; serverUrl: string; adminSecret: string },
  label: string,
  userId: string,
  dbName = uniqueDbName(`world-tour-${label}`),
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      jwtToken: await getJazzServerJwtForUser(userId, undefined, server.appId),
      driver: { type: "persistent", dbName },
    }),
  );
}
