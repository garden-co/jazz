import { afterEach, describe, expect, it } from "vitest";
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

const PAGE_SIZE = 12;

const cleanup = new TestCleanup();
afterEach(async () => cleanup.cleanup());

describe("BandBinder cross-topology recovery", () => {
  it("converges bounded workspace surfaces, offline work, and revocation exactly", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 41);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>> | undefined;
    let owner: Db | undefined;
    let manager: Db | undefined;
    let managerJwt = "";
    let managerDbName = "";
    let workspaceId = "";
    let managerMembershipId = "";
    let pageId = "";
    let taskBlockId = "";
    let offlineTaskId = "";
    let nestedPageId = "";
    let nestedBlockId = "";
    const taskSubscriptionSnapshots: string[][] = [];
    const expectedBlockIds = new Set<string>();

    const receipt = await runTopologyScenario(
      {
        id: "band-binder.workspace-recovery",
        topology: ["browser", "edge", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 41,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/band-binder/apps/next-betterauth test:browser:focused -- tests/browser/topology.e2e.test.tsx`,
        targets: {
          manager: {
            disconnect: async () => manager!.disconnect(),
            reconnect: async () => manager!.reconnect(),
            // A browser-runtime restart is deliberately distinct from a
            // transport reconnect: this exercises IndexedDB rehydration.
            restart: async () => {
              cleanup.untrack(manager!);
              await manager!.shutdown();
              manager = await openClient(server!, "manager", managerJwt, managerDbName);
            },
          },
          authorization: {
            failure: async () => {
              const token = await getJazzServerJwtForUser(
                "band-binder-outsider",
                undefined,
                server!.appId,
              );
              const outsider = await openClient(server!, "outsider", token);
              await expect(
                outsider
                  .insert(app.pages, { workspaceId, title: "Unauthorized rider" })
                  .wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        },
        phases: [
          {
            name: "bootstrap owner and stage manager",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("band-binder-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [ownerToken, managerToken] = await Promise.all([
                getJazzServerJwtForUser("band-binder-owner", undefined, server.appId),
                getJazzServerJwtForUser("band-binder-manager", undefined, server.appId),
              ]);
              managerJwt = managerToken;
              managerDbName = uniqueDbName("band-binder-manager");
              owner = await openClient(server, "owner", ownerToken);
              manager = await openClient(server, "manager", managerJwt, managerDbName);
              // Bootstrap is an explicit grant, not an implicit initial read.
              // A signed-in client starts with no workspace-visible state.
              expect(await manager.all(app.workspaces.where({}), { tier: "edge" })).toEqual([]);
              const workspace = await owner
                .insert(app.workspaces, {
                  name: "World tour",
                  ownerSubject: "band-binder-owner",
                })
                .wait({ tier: "edge" });
              workspaceId = workspace.id;
              await owner
                .insert(app.members, {
                  workspaceId,
                  subject: "band-binder-owner",
                  role: "owner",
                })
                .wait({ tier: "edge" });
              const membership = await owner
                .insert(app.members, {
                  workspaceId,
                  subject: "band-binder-manager",
                  role: "stage_manager",
                })
                .wait({ tier: "edge" });
              managerMembershipId = membership.id;
              await waitForQuery(
                manager,
                app.members.where({ id: managerMembershipId, subject: "band-binder-manager" }),
                (rows) => rows.length === 1,
                "manager receives own membership grant",
                15_000,
                "edge",
              );
              await waitForQuery(
                manager,
                app.workspaces.where({ id: workspaceId }),
                (rows) => rows.length === 1,
                "manager receives workspace",
                15_000,
                "edge",
              );
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "two clients create bounded ordered surfaces",
            run: async () => {
              const page = await manager!
                .insert(app.pages, { workspaceId, title: "Berlin" })
                .wait({ tier: "edge" });
              pageId = page.id;
              const nestedPage = await manager!
                .insert(app.pages, {
                  workspaceId,
                  parentPageId: pageId,
                  title: "Berlin / stage notes",
                })
                .wait({ tier: "edge" });
              nestedPageId = nestedPage.id;
              // PageNavigation owns this exact bounded, ordered child-page
              // query.  Seed one more row than the page so the receipt proves
              // both the ordering and the bound rather than merely eventual
              // delivery of a single child.
              await Promise.all(
                Array.from({ length: PAGE_SIZE }, (_, index) =>
                  manager!
                    .insert(app.pages, {
                      workspaceId,
                      parentPageId: pageId,
                      title: `Child page ${String(index).padStart(2, "0")}`,
                    })
                    .wait({ tier: "edge" }),
                ),
              );
              const [ownerBlock, managerBlock] = await Promise.all([
                owner!
                  .insert(app.blocks, {
                    workspaceId,
                    pageId,
                    position: 10,
                    kind: "song",
                    payload: { title: "Encore" },
                  })
                  .wait({ tier: "edge" }),
                manager!
                  .insert(app.blocks, {
                    workspaceId,
                    pageId,
                    position: 20,
                    kind: "task",
                    payload: { title: "Load in" },
                  })
                  .wait({ tier: "edge" }),
              ]);
              expectedBlockIds.add(ownerBlock.id);
              expectedBlockIds.add(managerBlock.id);
              taskBlockId = managerBlock.id;
              await Promise.all(
                Array.from({ length: PAGE_SIZE - 1 }, (_, index) =>
                  manager!
                    .insert(app.blocks, {
                      workspaceId,
                      pageId,
                      position: 30 + index * 10,
                      kind: "text",
                      payload: { text: `Checklist ${index}` },
                    })
                    .wait({ tier: "edge" }),
                ),
              );
              const nestedParent = await manager!
                .insert(app.blocks, {
                  workspaceId,
                  pageId: nestedPageId,
                  position: 10,
                  kind: "text",
                  payload: { text: "Venue notes" },
                })
                .wait({ tier: "edge" });
              const nestedBlock = await manager!
                .insert(app.blocks, {
                  workspaceId,
                  pageId: nestedPageId,
                  parentBlockId: nestedParent.id,
                  position: 20,
                  kind: "attachment",
                  payload: { caption: "Stage plot" },
                })
                .wait({ tier: "edge" });
              nestedBlockId = nestedBlock.id;
              await Promise.all([
                owner!
                  .insert(app.songs, {
                    workspaceId,
                    blockId: ownerBlock.id,
                    title: "Encore",
                    key: "D",
                  })
                  .wait({ tier: "edge" }),
                ...Array.from({ length: PAGE_SIZE }, (_, index) =>
                  manager!
                    .insert(app.songs, {
                      workspaceId,
                      blockId: ownerBlock.id,
                      title: `Song ${String(index).padStart(2, "0")}`,
                    })
                    .wait({ tier: "edge" }),
                ),
                manager!
                  .insert(app.calendarEvents, {
                    workspaceId,
                    blockId: managerBlock.id,
                    title: "Load in",
                    startsAt: new Date("2030-04-01T14:00:00Z"),
                    endsAt: new Date("2030-04-01T15:00:00Z"),
                  })
                  .wait({ tier: "edge" }),
                ...Array.from({ length: PAGE_SIZE }, (_, index) =>
                  manager!
                    .insert(app.calendarEvents, {
                      workspaceId,
                      blockId: managerBlock.id,
                      title: `Soundcheck ${String(index).padStart(2, "0")}`,
                      startsAt: new Date(`2030-04-${String(index + 2).padStart(2, "0")}T14:00:00Z`),
                      endsAt: new Date(`2030-04-${String(index + 2).padStart(2, "0")}T15:00:00Z`),
                    })
                    .wait({ tier: "edge" }),
                ),
                manager!
                  .insert(app.attachments, {
                    workspaceId,
                    blockId: nestedBlockId,
                    name: "stage-plot.txt",
                    mediaType: "text/plain",
                    bytes: new TextEncoder().encode("channels 1-16"),
                  })
                  .wait({ tier: "edge" }),
                ...Array.from({ length: PAGE_SIZE }, (_, index) =>
                  manager!
                    .insert(app.attachments, {
                      workspaceId,
                      blockId: nestedBlockId,
                      name: `asset-${String(index).padStart(2, "0")}.txt`,
                      mediaType: "text/plain",
                      bytes: new TextEncoder().encode(`asset ${index}`),
                    })
                    .wait({ tier: "edge" }),
                ),
              ]);
              const blocks = await waitForQuery(
                owner!,
                app.blocks
                  .where({ workspaceId, pageId })
                  .orderBy("position", "asc")
                  .offset(0)
                  .limit(PAGE_SIZE),
                (rows) => rows.length === PAGE_SIZE,
                "ordered blocks converge",
                15_000,
                "edge",
              );
              expect(blocks.map((block) => block.position)).toEqual(
                Array.from({ length: PAGE_SIZE }, (_, index) => 10 + index * 10),
              );
              expect(new Set(blocks.slice(0, 2).map((block) => block.id))).toEqual(
                expectedBlockIds,
              );
              const managerBlocks = await waitForQuery(
                manager!,
                app.blocks
                  .where({ workspaceId, pageId })
                  .orderBy("position", "asc")
                  .offset(1)
                  .limit(PAGE_SIZE),
                (rows) => rows.length === PAGE_SIZE,
                "manager receives the exact ordered block window",
                15_000,
                "edge",
              );
              expect(managerBlocks.map((block) => block.position)).toEqual(
                Array.from({ length: PAGE_SIZE }, (_, index) => 20 + index * 10),
              );
              expect(
                await waitForQuery(
                  owner!,
                  app.pages
                    .where({ workspaceId, parentPageId: pageId })
                    .orderBy("title", "asc")
                    .offset(1)
                    .limit(PAGE_SIZE),
                  (rows) =>
                    rows.length === PAGE_SIZE &&
                    rows.every(
                      (row, index) => row.title === `Child page ${String(index).padStart(2, "0")}`,
                    ),
                  "bounded child-page navigation follows its parent permission witness",
                  15_000,
                  "edge",
                ),
              ).toHaveLength(PAGE_SIZE);
              const nestedBlocks = await waitForQuery(
                owner!,
                app.blocks.where({ workspaceId, pageId: nestedPageId }).orderBy("position", "asc"),
                (rows) =>
                  rows.length === 2 &&
                  rows.some((block) => block.id === nestedBlockId && block.parentBlockId !== null),
                "nested page and block tree converge",
                15_000,
                "edge",
              );
              expect(nestedBlocks.map((block) => block.id)).toContain(nestedBlockId);
              expect(
                await waitForQuery(
                  owner!,
                  app.attachments
                    .where({ workspaceId, blockId: nestedBlockId })
                    .orderBy("name", "asc")
                    .limit(PAGE_SIZE),
                  (rows) =>
                    rows.length === PAGE_SIZE &&
                    rows.every(
                      (row, index) => row.name === `asset-${String(index).padStart(2, "0")}.txt`,
                    ),
                  "bounded attachment list follows its nested block permission witness",
                  15_000,
                  "edge",
                ),
              ).toHaveLength(PAGE_SIZE);
              const [songs, events] = await Promise.all([
                waitForQuery(
                  owner!,
                  app.songs.where({ workspaceId }).orderBy("title", "asc").limit(PAGE_SIZE),
                  (rows) => rows.length === PAGE_SIZE && rows[0]?.title === "Encore",
                  "bounded song index converges through the workspace permission",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  owner!,
                  app.calendarEvents
                    .where({ workspaceId })
                    .orderBy("startsAt", "asc")
                    .limit(PAGE_SIZE),
                  (rows) => rows.length === PAGE_SIZE && rows[0]?.title === "Load in",
                  "bounded calendar converges through the workspace permission",
                  15_000,
                  "edge",
                ),
              ]);
              expect(songs.map((song) => song.title)).toEqual([
                "Encore",
                ...Array.from(
                  { length: PAGE_SIZE - 1 },
                  (_, index) => `Song ${String(index).padStart(2, "0")}`,
                ),
              ]);
              expect(events.map((event) => event.title)).toEqual([
                "Load in",
                ...Array.from(
                  { length: PAGE_SIZE - 1 },
                  (_, index) => `Soundcheck ${String(index).padStart(2, "0")}`,
                ),
              ]);
              cleanup.trackSubscription(
                owner!.subscribeAll(app.tasks.where({ workspaceId }), (delta) => {
                  taskSubscriptionSnapshots.push(delta.all.map((task) => task.id).sort());
                }),
              );
            },
            faultsAfter: [{ kind: "disconnect", target: "manager" }],
          },
          {
            name: "stage manager writes locally while disconnected",
            run: async () => {
              const task = await manager!
                .insert(app.tasks, {
                  workspaceId,
                  blockId: taskBlockId,
                  title: "Confirm stage plot",
                  completed: false,
                })
                .wait({ tier: "local" });
              offlineTaskId = task.id;
              expect(await manager!.all(app.tasks.where({ id: offlineTaskId }))).toHaveLength(1);
            },
            faultsAfter: [{ kind: "reconnect", target: "manager" }],
          },
          {
            name: "offline work converges to the live owner subscription",
            run: async () => {
              const tasks = await waitForQuery(
                owner!,
                app.tasks.where({ workspaceId }).orderBy("title", "asc").limit(12),
                (rows) => rows.some((task) => task.id === offlineTaskId),
                "offline task converges",
                20_000,
                "edge",
              );
              expect(tasks.map((task) => task.id)).toEqual([offlineTaskId]);
              await waitForCondition(
                async () => taskSubscriptionSnapshots.some((ids) => ids.includes(offlineTaskId)),
                15_000,
                "owner task subscription publishes the converged offline task",
              );
            },
            faultsAfter: [{ kind: "restart", target: "manager" }],
          },
          {
            name: "persisted manager remount rehydrates then loses access on revocation",
            run: async () => {
              const persistedTasks = await waitForQuery(
                manager!,
                app.tasks.where({ workspaceId, id: offlineTaskId }),
                (rows) => rows.length === 1,
                "manager rehydrates accepted offline work after browser restart",
                15_000,
                "local",
              );
              expect(persistedTasks.map((task) => task.id)).toEqual([offlineTaskId]);
              await owner!.delete(app.members, managerMembershipId).wait({ tier: "edge" });
              const rejected = manager!.insert(app.pages, {
                workspaceId,
                title: "Rejected after revocation",
              });
              await expect(rejected.wait({ tier: "edge" })).rejects.toThrow();
              await waitForQuery(
                manager!,
                app.pages.where({ workspaceId, title: "Rejected after revocation" }),
                (rows) => rows.length === 0,
                "rejected optimistic page rolls back",
                15_000,
                "local",
              );
              expect(
                await owner!.all(
                  app.pages.where({ workspaceId, title: "Rejected after revocation" }),
                  { tier: "edge" },
                ),
              ).toEqual([]);
              await waitForQuery(
                manager!,
                app.tasks.where({ workspaceId, id: offlineTaskId }),
                (rows) => rows.length === 0,
                "revocation removes persisted task from the manager read surface",
                15_000,
                "edge",
              );
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
      ["restart", "completed"],
    ]);
  }, 90_000);
});

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`band-binder-${label}`),
): Promise<Db> {
  return cleanup.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName },
    }),
  );
}
