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
import { posterShopScenario } from "../../src/scenario.js";

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

/** Public browser -> edge -> core receipt. The application owns its workflow;
 * the shared harness supplies only deterministic timeouts and fault receipts. */
describe("PosterShop cross-topology recovery", () => {
  it("converges ordered canvas edits, presence and a local replay while enforcing revocation", async () => {
    const workload = posterShopScenario;
    const seed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 47);
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let canvas: { id: string };
    let layer: { id: string };
    let ownerShape: { id: string };
    let editorShape: { id: string };
    let offlineShape: { id: string };
    let editorMembership: { id: string };
    const receipt = await runTopologyScenario(
      {
        id: workload.id,
        topology: ["browser", "edge", "core"],
        seed: Number.isSafeInteger(seed) ? seed : 47,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/poster-shop/apps/nextjs-betterauth test:browser -- tests/browser/topology.e2e.test.tsx`,
        targets: {
          owner: {
            disconnect: async () => owner.disconnect(),
            reconnect: async () => owner.reconnect(),
          },
          authorization: {
            failure: async () => {
              const token = await getJazzServerJwtForUser(
                "poster-outsider",
                undefined,
                server.appId,
              );
              const outsider = await openClient(server, "outsider", token);
              await expect(
                outsider.insert(app.shapes, shape(canvas.id, layer.id, 99)).wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        },
        phases: [
          {
            name: "owner bootstrap and editor admission",
            run: async () => {
              server = await getJazzServerInfo(uniqueDbName("poster-shop-topology"));
              await deploy({
                appId: server.appId,
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
                schema: app,
                permissions,
              });
              const [ownerToken, editorToken] = await Promise.all([
                getJazzServerJwtForUser("poster-owner", undefined, server.appId),
                getJazzServerJwtForUser("poster-editor", undefined, server.appId),
              ]);
              owner = await openClient(server, "owner", ownerToken);
              editor = await openClient(server, "editor", editorToken);
              canvas = await owner
                .insert(app.canvases, { title: "Midnight", width: 1080, height: 1350 })
                .wait({ tier: "edge" });
              await owner
                .insert(app.canvasMembers, {
                  canvasId: canvas.id,
                  userId: "poster-owner",
                  role: "admin",
                })
                .wait({ tier: "edge" });
              editorMembership = await owner
                .insert(app.canvasMembers, {
                  canvasId: canvas.id,
                  userId: "poster-editor",
                  role: "editor",
                })
                .wait({ tier: "edge" });
              layer = await owner
                .insert(app.layers, {
                  canvasId: canvas.id,
                  name: "Artwork",
                  zIndex: 0,
                  visible: true,
                })
                .wait({ tier: "edge" });
              await waitForQuery(
                editor,
                app.layers.where({ canvasId: canvas.id }),
                (rows) => rows.length === 1,
                "editor receives canvas",
                15_000,
                "edge",
              );
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "concurrent ordered edits and checkpoint",
            run: async () => {
              const [createdOwner, createdEditor] = await Promise.all([
                owner.insert(app.shapes, shape(canvas.id, layer.id, 0)).wait({ tier: "edge" }),
                editor.insert(app.shapes, shape(canvas.id, layer.id, 1)).wait({ tier: "edge" }),
              ]);
              ownerShape = createdOwner;
              editorShape = createdEditor;
              await owner
                .insert(app.cursors, {
                  canvasId: canvas.id,
                  userId: "poster-owner",
                  x: 5,
                  y: 6,
                  color: "#f50",
                })
                .wait({ tier: "edge" });
              await owner
                .insert(app.checkpoints, { canvasId: canvas.id, label: "Approved", branch: "main" })
                .wait({ tier: "edge" });
            },
            faultsAfter: [{ kind: "disconnect", target: "owner" }],
          },
          {
            name: "offline local shape",
            run: async () => {
              offlineShape = await owner
                .insert(app.shapes, shape(canvas.id, layer.id, 2))
                .wait({ tier: "local" });
              expect(
                (await owner.all(app.shapes.where({ canvasId: canvas.id }))).map((row) => row.id),
              ).toContain(offlineShape.id);
            },
            faultsAfter: [{ kind: "reconnect", target: "owner" }],
          },
          {
            name: "peer convergence and revocation",
            run: async () => {
              const shapes = await waitForQuery(
                editor,
                app.shapes.where({ canvasId: canvas.id }).orderBy("zIndex", "asc"),
                (rows) => rows.length === 3,
                "editor receives replay",
                20_000,
                "edge",
              );
              expect(shapes.map((row) => [row.id, row.zIndex])).toEqual([
                [ownerShape.id, 0],
                [editorShape.id, 1],
                [offlineShape.id, 2],
              ]);
              expect(
                (
                  await editor.all(app.cursors.where({ canvasId: canvas.id }), { tier: "edge" })
                ).map((row) => row.userId),
              ).toEqual(["poster-owner"]);
              expect(
                (
                  await editor.all(app.checkpoints.where({ canvasId: canvas.id }), { tier: "edge" })
                ).map((row) => row.label),
              ).toEqual(["Approved"]);
              await owner.delete(app.canvasMembers, editorMembership.id).wait({ tier: "edge" });
              await expect(
                editor.insert(app.shapes, shape(canvas.id, layer.id, 3)).wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        ],
        cleanup: async () => ctx.cleanup(),
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
});

function shape(canvasId: string, layerId: string, zIndex: number) {
  return {
    canvasId,
    layerId,
    kind: "rect" as const,
    x: zIndex * 10,
    y: zIndex * 10,
    width: 20,
    height: 20,
    rotation: 0,
    zIndex,
    fill: "#ff5a36",
  };
}
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
      driver: { type: "persistent", dbName: uniqueDbName(`poster-shop-${label}`) },
    }),
  );
}
