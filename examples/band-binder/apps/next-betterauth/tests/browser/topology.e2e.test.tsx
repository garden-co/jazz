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

const cleanup = new TestCleanup();
afterEach(async () => cleanup.cleanup());

describe("BandBinder cross-topology recovery", () => {
  it("converges bounded workspace surfaces, offline work, and revocation exactly", async () => {
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 41);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>> | undefined;
    let owner: Db | undefined;
    let manager: Db | undefined;
    let workspaceId = "";
    let managerMembershipId = "";
    let pageId = "";
    let taskBlockId = "";
    let offlineTaskId = "";
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
              owner = await openClient(server, "owner", ownerToken);
              manager = await openClient(server, "manager", managerToken);
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
              await Promise.all([
                owner!
                  .insert(app.songs, {
                    workspaceId,
                    blockId: ownerBlock.id,
                    title: "Encore",
                    key: "D",
                  })
                  .wait({ tier: "edge" }),
                manager!
                  .insert(app.calendarEvents, {
                    workspaceId,
                    blockId: managerBlock.id,
                    title: "Load in",
                    startsAt: new Date("2030-04-01T14:00:00Z"),
                    endsAt: new Date("2030-04-01T15:00:00Z"),
                  })
                  .wait({ tier: "edge" }),
              ]);
              const blocks = await waitForQuery(
                owner!,
                app.blocks.where({ workspaceId, pageId }).orderBy("position", "asc").limit(12),
                (rows) => rows.length === 2,
                "ordered blocks converge",
                15_000,
                "edge",
              );
              expect(blocks.map((block) => block.position)).toEqual([10, 20]);
              expect(new Set(blocks.map((block) => block.id))).toEqual(expectedBlockIds);
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
            name: "offline work converges then membership revokes",
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
  }, 90_000);
});

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
): Promise<Db> {
  return cleanup.track(
    await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName: uniqueDbName(`band-binder-${label}`) },
    }),
  );
}
