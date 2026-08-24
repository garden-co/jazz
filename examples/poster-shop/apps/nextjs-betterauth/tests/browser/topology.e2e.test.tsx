import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../../../../../packages/jazz-tools/src/runtime/db.js";
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

declare const __JAZZ_EXAMPLE_TOPOLOGY_SEED__: string;

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

/** Public browser -> edge -> core receipt. The application owns its workflow;
 * the shared harness supplies only deterministic timeouts and fault receipts. */
describe("PosterShop cross-topology recovery", () => {
  it("converges ordered canvas edits, presence and a local replay while enforcing revocation", async () => {
    const workload = posterShopScenario;
    const requestedSeed = Number(__JAZZ_EXAMPLE_TOPOLOGY_SEED__);
    const seed = Number.isSafeInteger(requestedSeed) ? requestedSeed : 47;
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let ownerToken: string;
    let ownerDbName: string;
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
        seed,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/poster-shop/apps/nextjs-betterauth test:browser -- tests/browser/topology.e2e.test.tsx`,
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
              const [issuedOwnerToken, editorToken] = await Promise.all([
                getJazzServerJwtForUser("poster-owner", undefined, server.appId),
                getJazzServerJwtForUser("poster-editor", undefined, server.appId),
              ]);
              ownerToken = issuedOwnerToken;
              ownerDbName = uniqueDbName("poster-shop-owner");
              owner = await openClient(server, "owner", ownerToken, ownerDbName);
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
                canvasQueries(canvas.id).layers,
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
              await editor
                .insert(app.cursors, {
                  canvasId: canvas.id,
                  userId: "poster-editor",
                  x: 17,
                  y: 19,
                  color: "#58f",
                })
                .wait({ tier: "edge" });
              await owner
                .insert(app.assets, {
                  canvasId: canvas.id,
                  name: "headline.svg",
                  mimeType: "image/svg+xml",
                  byteLength: 512,
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
                (await owner.all(canvasQueries(canvas.id).shapes, { tier: "local" })).map(
                  (row) => row.id,
                ),
              ).toContain(offlineShape.id);
            },
            faultsAfter: [
              { kind: "reconnect", target: "owner" },
              { kind: "restart", target: "owner" },
            ],
          },
          {
            name: "persistent reopen and peer convergence",
            run: async () => {
              // These are the same parent-scoped, ordered reads used by the
              // rendered PosterShop components. Keeping the queries literal
              // here makes this a regression receipt for the app, not a
              // generic table-sync smoke test.
              const queries = canvasQueries(canvas.id);
              const reopened = await waitForQuery(
                owner,
                queries.shapes,
                (rows) => rows.some((row) => row.id === offlineShape.id),
                "persistent owner reopen retains offline shape",
                20_000,
                "edge",
              );
              expect(reopened.map((row) => [row.id, row.zIndex])).toContainEqual([
                offlineShape.id,
                2,
              ]);
              const shapes = await waitForQuery(
                editor,
                queries.shapes,
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
                (await editor.all(queries.cursors, { tier: "edge" })).map((row) => [
                  row.userId,
                  row.x,
                  row.y,
                ]),
              ).toEqual([
                ["poster-editor", 17, 19],
                ["poster-owner", 5, 6],
              ]);
              expect(
                (await editor.all(queries.assets, { tier: "edge" })).map((row) => row.name),
              ).toEqual(["headline.svg"]);
              expect(
                (await editor.all(queries.checkpoints, { tier: "edge" })).map((row) => row.label),
              ).toEqual(["Approved"]);
              expect(
                (await editor.all(queries.layers, { tier: "edge" })).map((row) => row.name),
              ).toEqual(["Artwork"]);
            },
          },
          {
            name: "revocation blocks a former editor",
            run: async () => {
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
      ["restart", "completed"],
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

function canvasQueries(canvasId: string) {
  return {
    layers: app.layers.where({ canvasId }).orderBy("zIndex", "asc"),
    shapes: app.shapes.where({ canvasId }).orderBy("zIndex", "asc"),
    assets: app.assets.where({ canvasId }).orderBy("name", "asc"),
    cursors: app.cursors.where({ canvasId }).orderBy("userId", "asc"),
    checkpoints: app.checkpoints.where({ canvasId }).orderBy("label", "asc"),
  };
}

async function openClient(
  server: { appId: string; serverUrl: string },
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`poster-shop-${label}`),
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
