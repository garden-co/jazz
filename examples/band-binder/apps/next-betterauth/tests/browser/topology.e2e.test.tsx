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
  blockJazzServerNetwork,
  getJazzServerInfo,
  getJazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";

const PAGE_SIZE = 12;

const cleanup = new TestCleanup();

afterEach(async () => {
  await cleanup.cleanup();
});

async function settle<T>(
  label: string,
  work: Promise<T>,
  timeoutMs = 15_000,
  diagnostics?: () => string,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () =>
            reject(
              new Error(
                `${label}: edge settlement did not complete after ${timeoutMs}ms` +
                  (diagnostics ? `; ${diagnostics()}` : ""),
              ),
            ),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

describe("BandBinder cross-topology recovery", () => {
  it("converges bounded workspace surfaces, offline work, and revocation exactly", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 41);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>> | undefined;
    let owner: Db | undefined;
    let manager: Db | undefined;
    let outsider: Db | undefined;
    let managerJwt = "";
    let managerDbName = "";
    let workspaceId = "";
    let managerMembershipId = "";
    let pageId = "";
    let taskBlockId = "";
    let offlineTaskId = "";
    let offlineChildPageId = "";
    let nestedPageId = "";
    let nestedBlockId = "";
    let nestedAttachmentId = "";
    const expectedChildPages: { id: string; title: string }[] = [];
    let expectedChildPagesBeforeReconnect: { id: string; title: string }[] = [];
    const ownerMutationErrors: unknown[] = [];
    const taskSubscriptionSnapshots: string[][] = [];
    const childPageWindowSubscriptionSnapshots: { id: string; title: string }[][] = [];
    const managerChildPageWindowSubscriptionSnapshots: { id: string; title: string }[][] = [];
    const expectedBlockIds = new Set<string>();
    const isExpectedChildPageWindow = (rows: { id: string; title: string }[]) =>
      rows.length === PAGE_SIZE &&
      rows.every(
        (row, index) =>
          row.id === expectedChildPages[index]?.id &&
          row.title === expectedChildPages[index]?.title,
      );

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
            restart: async ({ defer }) => {
              managerChildPageWindowSubscriptionSnapshots.length = 0;
              const serverUrl = server!.serverUrl;
              // Register this before acquiring the external route fault: a
              // failure in any later restart step must not leak the block into
              // another topology scenario.
              defer("unblock BandBinder Jazz server network", async () =>
                unblockJazzServerNetwork(serverUrl),
              );
              await blockJazzServerNetwork(serverUrl);
              await manager!.disconnect();
              await manager!.shutdown();
              cleanup.untrack(manager!);
              manager = await openClient(server!, "manager", managerJwt, managerDbName);
            },
          },
          serverNetwork: {
            reconnect: async () => {
              await unblockJazzServerNetwork(server!.serverUrl);
            },
          },
          authorization: {
            failure: async () => {
              const token = await getJazzServerJwtForUser(
                "band-binder-outsider",
                undefined,
                server!.appId,
              );
              outsider = await openClient(server!, "outsider", token);
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
              cleanup.trackSubscription(
                owner.onMutationError((event) => ownerMutationErrors.push(event)),
              );
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
              // The manager discovers their own grant first, then uses the
              // workspace witness to read the roster.  This is deliberately
              // broader than the self-grant check above: it catches a
              // maintained-query path that accidentally keeps the bootstrap
              // exception but drops ordinary role-scoped membership reads.
              expect(
                await waitForQuery(
                  manager,
                  app.members.where({ workspaceId }).orderBy("subject", "asc"),
                  (rows) =>
                    rows.length === 2 &&
                    rows.map((row) => row.subject).join(",") ===
                      "band-binder-manager,band-binder-owner",
                  "manager reads the workspace roster through its grant",
                  15_000,
                  "edge",
                ),
              ).toHaveLength(2);
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "two clients create bounded ordered surfaces",
            run: async () => {
              const page = await settle(
                "manager creates root page",
                manager!.insert(app.pages, { workspaceId, title: "Berlin" }).wait({ tier: "edge" }),
              );
              pageId = page.id;
              const nestedPage = await settle(
                "manager creates nested page",
                manager!
                  .insert(app.pages, {
                    workspaceId,
                    parentPageId: pageId,
                    title: "Berlin / stage notes",
                  })
                  .wait({ tier: "edge" }),
              );
              nestedPageId = nestedPage.id;
              // PageNavigation owns this exact bounded, ordered child-page
              // query.  Seed one more row than the page so the receipt proves
              // both the ordering and the bound rather than merely eventual
              // delivery of a single child.
              const childPages = await settle(
                "manager creates bounded child pages",
                Promise.all(
                  Array.from({ length: PAGE_SIZE }, (_, index) =>
                    manager!
                      .insert(app.pages, {
                        workspaceId,
                        parentPageId: pageId,
                        title: `Child page ${String(index).padStart(2, "0")}`,
                      })
                      .wait({ tier: "edge" }),
                  ),
                ),
              );
              expectedChildPages.push(
                ...childPages.map((childPage) => ({ id: childPage.id, title: childPage.title })),
              );
              await waitForQuery(
                owner!,
                app.pages.where({ id: pageId, workspaceId }),
                (rows) => rows.length === 1,
                "owner reads manager-created parent page before block write",
                15_000,
                "edge",
              );
              // Start the two independent writes together so this receipt keeps
              // exercising concurrent authors. Waiting locally first is only a
              // diagnostic boundary: edge settlement necessarily includes local
              // admission, and the labels distinguish a client-side stall from
              // an authority/delivery stall without weakening either assertion.
              const ownerBlockWrite = owner!.insert(app.blocks, {
                workspaceId,
                pageId,
                position: 10,
                kind: "song",
                payload: { title: "Encore" },
              });
              const managerBlockWrite = manager!.insert(app.blocks, {
                workspaceId,
                pageId,
                position: 20,
                kind: "task",
                payload: { title: "Load in" },
              });
              const [ownerBlock, managerBlock] = await Promise.all([
                settle(
                  "owner admits root block locally",
                  ownerBlockWrite.wait({ tier: "local" }),
                ).then(() =>
                  settle(
                    "owner settles root block at edge",
                    ownerBlockWrite.wait({ tier: "edge" }),
                    15_000,
                    () => `mutationErrors=${JSON.stringify(ownerMutationErrors)}`,
                  ),
                ),
                settle(
                  "manager admits root block locally",
                  managerBlockWrite.wait({ tier: "local" }),
                ).then(() =>
                  settle(
                    "manager settles root block at edge",
                    managerBlockWrite.wait({ tier: "edge" }),
                  ),
                ),
              ]);
              expectedBlockIds.add(ownerBlock.id);
              expectedBlockIds.add(managerBlock.id);
              taskBlockId = managerBlock.id;
              await settle(
                "manager creates bounded checklist blocks",
                Promise.all(
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
                ),
              );
              const nestedParent = await settle(
                "manager creates nested parent block",
                manager!
                  .insert(app.blocks, {
                    workspaceId,
                    pageId: nestedPageId,
                    position: 10,
                    kind: "text",
                    payload: { text: "Venue notes" },
                  })
                  .wait({ tier: "edge" }),
              );
              const nestedBlock = await settle(
                "manager creates nested attachment block",
                manager!
                  .insert(app.blocks, {
                    workspaceId,
                    pageId: nestedPageId,
                    parentBlockId: nestedParent.id,
                    position: 20,
                    kind: "attachment",
                    payload: { caption: "Stage plot" },
                  })
                  .wait({ tier: "edge" }),
              );
              nestedBlockId = nestedBlock.id;
              await settle(
                "clients create bounded songs calendar and attachments",
                Promise.all([
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
                        startsAt: new Date(
                          `2030-04-${String(index + 2).padStart(2, "0")}T14:00:00Z`,
                        ),
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
                    .wait({ tier: "edge" })
                    .then((attachment) => {
                      nestedAttachmentId = attachment.id;
                    }),
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
                ]),
              );
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
                  isExpectedChildPageWindow,
                  "bounded child-page navigation follows its parent permission witness",
                  15_000,
                  "edge",
                ),
              ).toHaveLength(PAGE_SIZE);
              // Keep a second, independently shaped subscriber alive across
              // the manager's partition. Its window is exactly the page
              // navigation shape, while the task subscriber below exercises
              // a separate collection and mutation stream.
              cleanup.trackSubscription(
                owner!.subscribeAll(childPageWindow(workspaceId, pageId), (delta) => {
                  childPageWindowSubscriptionSnapshots.push(
                    delta.all.map(({ id, title }) => ({ id, title })),
                  );
                }),
              );
              cleanup.trackSubscription(
                manager!.subscribeAll(childPageWindow(workspaceId, pageId), (delta) => {
                  managerChildPageWindowSubscriptionSnapshots.push(
                    delta.all.map(({ id, title }) => ({ id, title })),
                  );
                }),
              );
              await waitForCondition(
                async () =>
                  [
                    childPageWindowSubscriptionSnapshots,
                    managerChildPageWindowSubscriptionSnapshots,
                  ].every((snapshots) => isExpectedChildPageWindow(snapshots.at(-1) ?? [])),
                15_000,
                "both child-page subscriptions start with the exact bounded window",
              );
              const [stagePlot] = await waitForQuery(
                owner!,
                app.attachments.where({
                  id: nestedAttachmentId,
                  workspaceId,
                  blockId: nestedBlockId,
                }),
                (rows) => rows.length === 1,
                "nested attachment bytes propagate to another workspace member",
                15_000,
                "edge",
              );
              expect(stagePlot?.bytes).toEqual(new TextEncoder().encode("channels 1-16"));
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
                  "bounded attachment list follows the workspace membership permission",
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
              // This alphabetically precedes the existing visible window.
              // Updating the expected sequence now gives the reconnect phase
              // an exact shifting-window oracle rather than a weak length
              // assertion.
              expectedChildPagesBeforeReconnect = expectedChildPages.map((page) => ({ ...page }));
              const childPage = await manager!
                .insert(app.pages, {
                  workspaceId,
                  parentPageId: pageId,
                  title: "Child page -1",
                })
                .wait({ tier: "local" });
              offlineChildPageId = childPage.id;
              expectedChildPages.unshift({ id: childPage.id, title: childPage.title });
              expectedChildPages.pop();
              expect(
                (await manager!.all(childPageWindow(workspaceId, pageId), { tier: "local" })).map(
                  ({ id, title }) => ({ id, title }),
                ),
              ).toEqual(expectedChildPages);
              // The manager's optimistic writes must remain isolated while
              // its transport is disconnected. These edge reads are the
              // negative half of the subsequent convergence assertions.
              await expect(
                owner!.all(app.tasks.where({ workspaceId, id: offlineTaskId }), { tier: "edge" }),
              ).resolves.toEqual([]);
              expect(
                (await owner!.all(childPageWindow(workspaceId, pageId), { tier: "edge" })).map(
                  ({ id, title }) => ({ id, title }),
                ),
              ).toEqual(expectedChildPagesBeforeReconnect);
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
                async () => {
                  const latest = taskSubscriptionSnapshots.at(-1);
                  return latest?.length === 1 && latest[0] === offlineTaskId;
                },
                15_000,
                "owner task subscription publishes the converged offline task",
              );
              const shiftedChildWindow = await waitForQuery(
                owner!,
                childPageWindow(workspaceId, pageId),
                isExpectedChildPageWindow,
                "offline child page shifts the owner bounded navigation window",
                20_000,
                "edge",
              );
              expect(shiftedChildWindow[0]).toMatchObject({ id: offlineChildPageId });
              await waitForCondition(
                async () =>
                  isExpectedChildPageWindow(childPageWindowSubscriptionSnapshots.at(-1) ?? []),
                15_000,
                "owner child-page subscription publishes the recovered bounded-window shift",
              );
              await waitForQuery(
                manager!,
                childPageWindow(workspaceId, pageId),
                isExpectedChildPageWindow,
                "manager settles the recovered bounded-window shift at edge",
                15_000,
                "edge",
              );
              await waitForCondition(
                async () =>
                  isExpectedChildPageWindow(
                    managerChildPageWindowSubscriptionSnapshots.at(-1) ?? [],
                  ),
                15_000,
                "manager child-page subscription publishes the recovered bounded-window shift",
              );
            },
            faultsAfter: [{ kind: "restart", target: "manager" }],
          },
          {
            name: "offline persisted manager remount rehydrates exact local surfaces",
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
              const [persistedStagePlot] = await waitForQuery(
                manager!,
                app.attachments.where({
                  id: nestedAttachmentId,
                  workspaceId,
                  blockId: nestedBlockId,
                }),
                (rows) => rows.length === 1,
                "manager rehydrates the nested attachment after browser restart",
                15_000,
                "local",
              );
              expect(persistedStagePlot?.bytes).toEqual(new TextEncoder().encode("channels 1-16"));
              const persistedChildPages = await waitForQuery(
                manager!,
                app.pages
                  .where({ workspaceId, parentPageId: pageId })
                  .orderBy("title", "asc")
                  .offset(1)
                  .limit(PAGE_SIZE),
                isExpectedChildPageWindow,
                "manager rehydrates the exact bounded child-page window after browser restart",
                15_000,
                "local",
              );
              expect(persistedChildPages.map(({ id, title }) => ({ id, title }))).toEqual(
                expectedChildPages,
              );
            },
            faultsAfter: [
              { kind: "reconnect", target: "serverNetwork" },
              { kind: "reconnect", target: "manager" },
            ],
          },
          {
            name: "reconnected manager settles then loses live and persisted access",
            run: async () => {
              await waitForQuery(
                manager!,
                app.tasks.where({ workspaceId, id: offlineTaskId }),
                (rows) => rows.length === 1,
                "reconnected manager settles its persisted task at edge",
                15_000,
                "edge",
              );
              const settledChildPages = await waitForQuery(
                manager!,
                childPageWindow(workspaceId, pageId),
                isExpectedChildPageWindow,
                "reconnected manager settles the exact bounded child-page window at edge",
                15_000,
                "edge",
              );
              expect(settledChildPages.map(({ id, title }) => ({ id, title }))).toEqual(
                expectedChildPages,
              );
              // Restart creates a new client instance, so this is a fresh
              // subscription on its rehydrated cache. It must observe the
              // same window before revocation and then receive the removal.
              cleanup.trackSubscription(
                manager!.subscribeAll(childPageWindow(workspaceId, pageId), (delta) => {
                  managerChildPageWindowSubscriptionSnapshots.push(
                    delta.all.map(({ id, title }) => ({ id, title })),
                  );
                }),
              );
              await waitForCondition(
                async () =>
                  isExpectedChildPageWindow(
                    managerChildPageWindowSubscriptionSnapshots.at(-1) ?? [],
                  ),
                15_000,
                "rehydrated manager subscription starts with the exact bounded window",
              );
              const revocationSnapshotCursor = managerChildPageWindowSubscriptionSnapshots.length;
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
              await Promise.all([
                waitForQuery(
                  manager!,
                  app.pages.where({ id: nestedPageId, workspaceId }),
                  (rows) => rows.length === 0,
                  "revocation removes the nested page from the manager read surface",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  manager!,
                  app.pages
                    .where({ workspaceId, parentPageId: pageId })
                    .orderBy("title", "asc")
                    .offset(1)
                    .limit(PAGE_SIZE),
                  (rows) => rows.length === 0,
                  "revocation removes the bounded child-page window from the manager read surface",
                  15_000,
                  "edge",
                ),
                waitForQuery(
                  manager!,
                  app.attachments.where({
                    id: nestedAttachmentId,
                    workspaceId,
                    blockId: nestedBlockId,
                  }),
                  (rows) => rows.length === 0,
                  "revocation removes the nested attachment from the manager read surface",
                  15_000,
                  "edge",
                ),
              ]);
              await waitForCondition(
                async () =>
                  managerChildPageWindowSubscriptionSnapshots.length > revocationSnapshotCursor &&
                  managerChildPageWindowSubscriptionSnapshots.at(-1)?.length === 0,
                15_000,
                "revocation publishes an empty bounded child-page subscription to the manager",
              );
            },
          },
        ],
        cleanup: async () => {
          const errors: Error[] = [];
          for (const [label, db] of [
            ["outsider", outsider],
            ["manager", manager],
            ["owner", owner],
          ] as const) {
            if (!db) continue;
            try {
              await db.shutdown();
            } catch (error) {
              errors.push(new Error(`failed to shut down BandBinder ${label}`, { cause: error }));
            } finally {
              cleanup.untrack(db);
            }
          }
          await cleanup.cleanup();
          if (errors.length > 0) {
            throw new AggregateError(errors, "BandBinder topology cleanup failed");
          }
        },
        cleanupTimeoutMs: 15_000,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["reconnect", "completed"],
      ["restart", "completed"],
      ["reconnect", "completed"],
      ["reconnect", "completed"],
    ]);
  }, 270_000);
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

function childPageWindow(workspaceId: string, parentPageId: string) {
  return app.pages
    .where({ workspaceId, parentPageId })
    .orderBy("title", "asc")
    .offset(1)
    .limit(PAGE_SIZE);
}
