import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { createDb, type Db } from "../../../../../../packages/jazz-tools/src/runtime/db.js";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import {
  TestCleanup,
  sleep,
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
import { posterShopScenario } from "../../src/scenario.js";

declare const __JAZZ_EXAMPLE_TOPOLOGY_SEED__: string;

const peerCommands = commands as unknown as {
  posterShopOpenConnectedPeer(input: {
    id: string;
    appId: string;
    dbName: string;
    table: string;
    queryJson?: string;
    schemaJson: string;
    serverUrl: string;
    jwtToken?: string;
    adminSecret?: string;
  }): Promise<void>;
  posterShopQueryConnectedPeer(id: string): Promise<Record<string, unknown>[]>;
  posterShopInsertConnectedPeer(
    id: string,
    table: string,
    row: Record<string, unknown>,
  ): Promise<string>;
  posterShopCloseConnectedPeer(id: string): Promise<void>;
};

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

/**
 * Public browser -> serving core receipt. The app owns the same parent-scoped
 * ordered reads as its components; the shared harness supplies only bounded
 * fault execution and replay receipts.
 */
describe("PosterShop cross-topology recovery", () => {
  it("replays an offline canvas edit across restart and rejects a revoked editor", async () => {
    const requestedSeed = Number(__JAZZ_EXAMPLE_TOPOLOGY_SEED__);
    const seed = Number.isSafeInteger(requestedSeed) ? requestedSeed : 47;
    let server: Awaited<ReturnType<typeof getJazzServerInfo>>;
    let owner: Db;
    let editor: Db;
    let reader: Db;
    let ownerToken: string;
    let ownerDbName: string;
    let canvas: { id: string };
    let layer: { id: string };
    let offlineShape: { id: string };
    let connectedShapeId: string;
    let editorMembership: { id: string };
    const connectedPeerId = uniqueDbName("poster-shop-connected-peer");
    const connectedWriterId = uniqueDbName("poster-shop-connected-writer");
    let connectedPeerInput: Parameters<typeof peerCommands.posterShopOpenConnectedPeer>[0];
    const windowSnapshots: Array<Array<{ id: string; zIndex: number }>> = [];

    const receipt = await runTopologyScenario(
      {
        id: posterShopScenario.id,
        topology: ["browser", "core"],
        seed,
        phaseTimeoutMs: 25_000,
        faultTimeoutMs: 15_000,
        replay: `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --dir examples/poster-shop/apps/nextjs-betterauth exec vitest run --config vitest.config.browser.ts`,
        targets: {
          owner: {
            disconnect: async ({ defer }) => {
              defer("unblock PosterShop Jazz server route", async () => {
                await unblockJazzServerNetwork(server.serverUrl);
              });
              await blockJazzServerNetwork(server.serverUrl);
              await owner.disconnect();
            },
            reconnect: async () => {
              await unblockJazzServerNetwork(server.serverUrl);
              await owner.reconnect();
            },
            restart: async () => {
              await owner.shutdown();
              ctx.untrack(owner);
              owner = await openClient(
                server.appId,
                server.serverUrl,
                "owner",
                ownerToken,
                ownerDbName,
              );
            },
          },
          authorization: {
            failure: async () => {
              const token = await getJazzServerJwtForUser(
                "poster-outsider",
                undefined,
                server.appId,
              );
              const outsider = await openClient(server.appId, server.serverUrl, "outsider", token);
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
              const [issuedOwnerToken, editorToken, readerToken] = await Promise.all([
                getJazzServerJwtForUser("poster-owner", undefined, server.appId),
                getJazzServerJwtForUser("poster-editor", undefined, server.appId),
                getJazzServerJwtForUser("poster-reader", undefined, server.appId),
              ]);
              ownerToken = issuedOwnerToken;
              ownerDbName = uniqueDbName("poster-shop-owner");
              owner = await openClient(
                server.appId,
                server.serverUrl,
                "owner",
                ownerToken,
                ownerDbName,
              );
              editor = await openClient(server.appId, server.serverUrl, "editor", editorToken);
              reader = await openClient(server.appId, server.serverUrl, "reader", readerToken);
              canvas = await owner
                .insert(app.canvases, { title: "Midnight", width: 1080, height: 1350 })
                .wait({ tier: "global" });
              await owner
                .insert(app.canvasMembers, {
                  canvasId: canvas.id,
                  memberAuthor: authorFromToken(ownerToken),
                  role: "admin",
                })
                .wait({ tier: "global" });
              editorMembership = await owner
                .insert(app.canvasMembers, {
                  canvasId: canvas.id,
                  memberAuthor: authorFromToken(editorToken),
                  role: "editor",
                })
                .wait({ tier: "global" });
              await owner
                .insert(app.canvasMembers, {
                  canvasId: canvas.id,
                  memberAuthor: authorFromToken(readerToken),
                  role: "viewer",
                })
                .wait({ tier: "global" });
              layer = await owner
                .insert(app.layers, {
                  canvasId: canvas.id,
                  name: "Artwork",
                  zIndex: 0,
                  visible: true,
                })
                .wait({ tier: "global" });
              await waitForQuery(
                reader,
                canvasQueries(canvas.id).layers,
                (rows) => rows.length === 1,
                "reader receives canvas layer",
                15_000,
                "edge",
              );
              ctx.trackSubscription(
                reader.subscribeAll(
                  canvasQueries(canvas.id).shapeWindow,
                  (delta) => {
                    windowSnapshots.push(
                      (delta.all ?? []).map((row) => ({ id: row.id, zIndex: row.zIndex })),
                    );
                  },
                  { tier: "edge" },
                ),
              );
              await waitForCondition(
                async () => windowSnapshots.length > 0,
                10_000,
                "reader shape-window subscription did not produce an initial snapshot",
              );
              connectedPeerInput = {
                id: connectedPeerId,
                appId: server.appId,
                dbName: uniqueDbName("poster-shop-connected-peer-store"),
                table: "shapes",
                queryJson: canvasQueries(canvas.id).shapes._build(),
                schemaJson: JSON.stringify(app.wasmSchema),
                serverUrl: server.serverUrl,
                jwtToken: readerToken,
              };
              await peerCommands.posterShopOpenConnectedPeer(connectedPeerInput);
              await peerCommands.posterShopOpenConnectedPeer({
                id: connectedWriterId,
                appId: server.appId,
                dbName: uniqueDbName("poster-shop-connected-writer-store"),
                table: "shapes",
                schemaJson: JSON.stringify(app.wasmSchema),
                serverUrl: server.serverUrl,
                adminSecret: server.adminSecret,
              });
            },
            faultsAfter: [{ kind: "failure", target: "authorization" }],
          },
          {
            name: "concurrent ordered canvas edits",
            run: async () => {
              const [ownerShape, editorShape] = await Promise.all([
                owner.insert(app.shapes, shape(canvas.id, layer.id, 0)).wait({ tier: "edge" }),
                editor.insert(app.shapes, shape(canvas.id, layer.id, 1)).wait({ tier: "edge" }),
              ]);
              await owner.insert(app.shapes, shape(canvas.id, layer.id, 3)).wait({ tier: "edge" });
              await owner
                .insert(app.checkpoints, { canvasId: canvas.id, label: "Approved", branch: "main" })
                .wait({ tier: "edge" });
              expect([ownerShape.zIndex, editorShape.zIndex]).toEqual([0, 1]);
            },
          },
          {
            name: "revoke editor before owner lifecycle fault",
            run: async () => {
              const authority = ctx.track(
                await createDb({
                  appId: server.appId,
                  serverUrl: server.serverUrl,
                  adminSecret: server.adminSecret,
                  driver: { type: "memory" },
                }),
              );
              await authority.delete(app.canvasMembers, editorMembership.id).wait({ tier: "edge" });
            },
            faultsAfter: [{ kind: "disconnect", target: "owner" }],
          },
          {
            name: "offline local shape remains peer-private",
            run: async () => {
              offlineShape = await owner
                .insert(app.shapes, shape(canvas.id, layer.id, 2))
                .wait({ tier: "local" });
              expect(
                (await owner.all(canvasQueries(canvas.id).shapes, { tier: "local" })).map(
                  (row) => row.id,
                ),
              ).toContain(offlineShape.id);
              // This peer owns a distinct Playwright BrowserContext, so route
              // interception on the owner's context cannot partition it too. Force
              // a new socket after the partition so a shared-context peer could not
              // satisfy this receipt from a connection opened before interception.
              await peerCommands.posterShopCloseConnectedPeer(connectedPeerId);
              await peerCommands.posterShopOpenConnectedPeer(connectedPeerInput);
              connectedShapeId = await peerCommands.posterShopInsertConnectedPeer(
                connectedWriterId,
                "shapes",
                shape(canvas.id, layer.id, 4),
              );
              await waitForCondition(
                async () =>
                  (await peerCommands.posterShopQueryConnectedPeer(connectedPeerId)).some(
                    (row) => row.id === connectedShapeId,
                  ),
                10_000,
                "independently connected peer did not receive the control write",
              );
              expect(
                (await owner.all(canvasQueries(canvas.id).shapes, { tier: "local" })).map(
                  (row) => row.id,
                ),
              ).not.toContain(connectedShapeId);
              // Repeated reads are also a negative control against an in-flight send race.
              for (let attempt = 0; attempt < 5; attempt += 1) {
                expect(
                  (await peerCommands.posterShopQueryConnectedPeer(connectedPeerId)).map(
                    (row) => row.id,
                  ),
                ).not.toContain(offlineShape.id);
                await sleep(150);
              }
            },
            faultsAfter: [{ kind: "restart", target: "owner" }],
          },
          {
            name: "persistent reopen retains offline local state",
            run: async () => {
              const reopened = await owner.all(canvasQueries(canvas.id).shapes, { tier: "local" });
              expect(reopened.map((row) => [row.id, row.zIndex])).toContainEqual([
                offlineShape.id,
                2,
              ]);
            },
            faultsAfter: [{ kind: "reconnect", target: "owner" }],
          },
          {
            name: "peer convergence retains ordered page and checkpoint",
            run: async () => {
              const queries = canvasQueries(canvas.id);
              const shapes = await waitForQuery(
                reader,
                queries.shapes,
                (rows) => rows.length === 5,
                "reader receives offline replay",
                20_000,
                "edge",
              );
              expect(shapes.map((row) => row.zIndex)).toEqual([0, 1, 2, 3, 4]);
              await waitForCondition(
                async () =>
                  windowSnapshots.some(
                    (rows) => rows.length === 2 && rows[0]?.zIndex === 1 && rows[1]?.zIndex === 2,
                  ),
                20_000,
                "reader bounded shape window did not receive the offline replay",
              );
              expect(
                (await reader.all(queries.shapeWindow, { tier: "edge" })).map((row) => row.zIndex),
              ).toEqual([1, 2]);
              expect(
                (await reader.all(queries.checkpoints, { tier: "edge" })).map((row) => row.label),
              ).toEqual(["Approved"]);
            },
          },
          {
            name: "revocation blocks a former editor",
            run: async () => {
              const coreEditor = await openClient(
                server.appId,
                server.serverUrl,
                "revoked-core-editor",
                await getJazzServerJwtForUser("poster-editor", undefined, server.appId),
              );
              await expect(
                coreEditor.insert(app.shapes, shape(canvas.id, layer.id, 5)).wait({ tier: "edge" }),
              ).rejects.toThrow();
            },
          },
        ],
        cleanup: async () => {
          await peerCommands.posterShopCloseConnectedPeer(connectedPeerId);
          await peerCommands.posterShopCloseConnectedPeer(connectedWriterId);
          await ctx.cleanup();
        },
        cleanupTimeoutMs: 10_000,
      },
      browserTopologyReporter,
    );
    expect(receipt.status).toBe("passed");
    expect(receipt.faults.map((fault) => [fault.kind, fault.status])).toEqual([
      ["failure", "completed"],
      ["disconnect", "completed"],
      ["restart", "completed"],
      ["reconnect", "completed"],
    ]);
  }, 75_000);
});

function authorFromToken(token: string): string {
  const claims = JSON.parse(atob(token.split(".")[1]!)) as { iss: string; sub: string };
  return JSON.stringify([claims.iss, claims.sub]);
}

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
    shapeWindow: app.shapes.where({ canvasId }).orderBy("zIndex", "asc").offset(1).limit(2),
    checkpoints: app.checkpoints.where({ canvasId }).orderBy("label", "asc"),
  };
}

async function openClient(
  appId: string,
  serverUrl: string,
  label: string,
  jwtToken: string,
  dbName = uniqueDbName(`poster-shop-${label}`),
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      jwtToken,
      driver: { type: "persistent", dbName },
    }),
  );
}
