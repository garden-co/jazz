import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  schema,
  type CompiledPermissions,
  type Db,
} from "../../src/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { TestCleanup, uniqueDbName, waitForCondition } from "./support.js";
import {
  blockJazzServerNetwork,
  getJazzServerTopologyInfo,
  restartJazzServerTopologyEdge,
  unblockJazzServerNetwork,
} from "./testing-server.js";
import { browserTopologyReporter, runTopologyScenario } from "./topology-harness.js";

declare const __JAZZ_EXAMPLE_TOPOLOGY_SEED__: string;

type MusicAgentModules = {
  app: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/schema.js"))["app"];
  DeterministicMusicAgent: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"))["DeterministicMusicAgent"];
  JazzMusicStore: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"))["JazzMusicStore"];
};

const activeCleanups = new Set<TestCleanup>();
afterEach(async () => {
  await Promise.all([...activeCleanups].map((cleanup) => cleanup.cleanup()));
  activeCleanups.clear();
});

describe("MusicAgent adopter E2E", () => {
  // #1844: streamed Text must cross the subscription/public binding as
  // logical text, never as an engine-owned indirect scalar descriptor.
  it("keeps an offline transcript local, then converges exactly once after reconnect", async () => {
    const ctx = new TestCleanup();
    activeCleanups.add(ctx);
    const seed = topologySeed();
    const replay = musicAgentReplay(seed, "keeps an offline transcript local");
    let writer: Db | undefined;
    let reader: Db | undefined;
    let conversation = "";
    let topology: Awaited<ReturnType<typeof getJazzServerTopologyInfo>> | undefined;
    let app: MusicAgentModules["app"] | undefined;
    let DeterministicMusicAgent: MusicAgentModules["DeterministicMusicAgent"] | undefined;
    let JazzMusicStore: MusicAgentModules["JazzMusicStore"] | undefined;
    let receivedTranscriptSnapshot = false;
    let observedTranscript: Array<{ id: string; role: string }> = [];
    await runTopologyScenario(
      {
        id: "music-agent.browser.core-peer-edges.offline-streamed-transcript",
        topology: ["core", "edge", "peer-edge", "browser"],
        seed,
        phaseTimeoutMs: 20_000,
        faultTimeoutMs: 20_000,
        replay,
        targets: {
          writerEdge: {
            failure: async ({ signal }) => {
              signal.throwIfAborted();
              await writer!.disconnect();
              await blockJazzServerNetwork(topology!.edgeUrl);
            },
            restart: async ({ signal }) => {
              signal.throwIfAborted();
              const restarted = await restartJazzServerTopologyEdge(topology!.topologyId, "edge");
              expect(restarted.edgeUrl).toBe(topology!.edgeUrl);
            },
            reconnect: async ({ signal }) => {
              signal.throwIfAborted();
              await unblockJazzServerNetwork(topology!.edgeUrl);
              await writer!.reconnect();
            },
          },
        },
        phases: [
          {
            name: "start core and peer edges and publish the MusicAgent schema",
            run: async () => {
              const modules = await loadMusicAgentModules();
              app = modules.app;
              DeterministicMusicAgent = modules.DeterministicMusicAgent;
              JazzMusicStore = modules.JazzMusicStore;
              const permissions = defineMusicAgentPermissions(app);
              topology = await getJazzServerTopologyInfo(uniqueDbName("music-agent-e2e"));
              await publish(
                topology.appId,
                topology.coreUrl,
                topology.adminSecret,
                app.wasmSchema,
                permissions,
              );
            },
            // Restart before browser clients attach. This proves the real edge
            // lifecycle while keeping the assertion below focused on the
            // streamed-value boundary rather than a second reconnect defect.
            faultsAfter: [{ kind: "restart", target: "writerEdge" }],
          },
          {
            name: "open browser clients on separate peer edges and establish subscriptions",
            run: async () => {
              const secret = generateAuthSecret();
              writer = await openDb(
                ctx,
                topology.appId,
                topology.edgeUrl,
                topology.adminSecret,
                "music-agent-writer",
                secret,
              );
              reader = await openDb(
                ctx,
                topology.appId,
                topology.peerEdgeUrl,
                topology.adminSecret,
                "music-agent-reader",
                secret,
              );
              conversation = await new JazzMusicStore(writer).createConversation(
                "Offline listening",
              );
              await writer
                .insert(app.conversations, { title: "connection barrier", created_at: new Date() })
                .wait({ tier: "global" });
              await reader
                .insert(app.conversations, {
                  title: "reader connection barrier",
                  created_at: new Date(),
                })
                .wait({ tier: "global" });
              ctx.trackSubscription(
                reader.subscribeAll(app.turns.where({ conversation_id: conversation }), (delta) => {
                  receivedTranscriptSnapshot = true;
                  observedTranscript = delta.all.map((turn) => ({ id: turn.id, role: turn.role }));
                }),
              );
              await waitForCondition(
                async () => receivedTranscriptSnapshot && observedTranscript.length === 0,
                10_000,
                "reader transcript subscription did not produce an empty initial snapshot",
              );
            },
            faultsAfter: [{ kind: "failure", target: "writerEdge" }],
          },
          {
            name: "write a MusicAgent transcript while the writer edge is actually unreachable",
            run: async () => {
              expect(
                await new DeterministicMusicAgent!(new JazzMusicStore!(writer!)).answer(
                  conversation,
                  "wind-down piano",
                ),
              ).toHaveLength(3);
              expect(observedTranscript).toEqual([]);
            },
          },
          {
            name: "restore the writer edge network and reconnect the browser client",
            run: async () => undefined,
            faultsAfter: [{ kind: "reconnect", target: "writerEdge" }],
          },
          {
            name: "flush through the restarted edge and observe the peer-edge subscription",
            run: async () => {
              await writer!.all(app!.turns.where({ conversation_id: conversation }), {
                tier: "global",
              });
              await waitForCondition(
                async () => observedTranscript.length === 3,
                15_000,
                "reader subscription did not receive offline MusicAgent transcript",
              );
              expect(observedTranscript.map((turn) => turn.role)).toEqual([
                "user",
                "assistant",
                "tool",
              ]);
              expect(new Set(observedTranscript.map((turn) => turn.id)).size).toBe(3);
            },
          },
        ],
        cleanup: async () => {
          if (topology) await unblockJazzServerNetwork(topology.edgeUrl);
          await ctx.cleanup();
        },
      },
      browserTopologyReporter,
    );
  }, 60_000);

  // #1844: retain the direct public read boundary separately from the
  // subscription scenario; either path must hydrate streamed Text.
  it("returns streamed transcript text from a reader edge query after reconnect", async () => {
    const ctx = new TestCleanup();
    activeCleanups.add(ctx);
    const seed = topologySeed();
    const replay = musicAgentReplay(seed, "returns streamed transcript text");
    let writer: Db | undefined;
    let reader: Db | undefined;
    let topology: Awaited<ReturnType<typeof getJazzServerTopologyInfo>> | undefined;
    let app: MusicAgentModules["app"] | undefined;
    let DeterministicMusicAgent: MusicAgentModules["DeterministicMusicAgent"] | undefined;
    let JazzMusicStore: MusicAgentModules["JazzMusicStore"] | undefined;
    let conversation = "";
    await runTopologyScenario(
      {
        id: "music-agent.browser.core-peer-edges.direct-streamed-read",
        topology: ["core", "edge", "peer-edge", "browser"],
        seed,
        phaseTimeoutMs: 20_000,
        faultTimeoutMs: 20_000,
        replay,
        targets: {
          writerEdge: {
            disconnect: async ({ signal }) => {
              signal.throwIfAborted();
              await writer!.disconnect();
              await blockJazzServerNetwork(topology!.edgeUrl);
            },
            reconnect: async ({ signal }) => {
              signal.throwIfAborted();
              await unblockJazzServerNetwork(topology!.edgeUrl);
              await writer!.reconnect();
            },
          },
        },
        phases: [
          {
            name: "start a core with peer edges and open a direct core reader",
            run: async () => {
              const modules = await loadMusicAgentModules();
              app = modules.app;
              DeterministicMusicAgent = modules.DeterministicMusicAgent;
              JazzMusicStore = modules.JazzMusicStore;
              topology = await getJazzServerTopologyInfo(uniqueDbName("music-agent-direct-read"));
              await publish(
                topology.appId,
                topology.coreUrl,
                topology.adminSecret,
                app.wasmSchema,
                defineMusicAgentPermissions(app),
              );
              const secret = generateAuthSecret();
              writer = await openDb(
                ctx,
                topology.appId,
                topology.edgeUrl,
                topology.adminSecret,
                "music-agent-direct-writer",
                secret,
              );
              reader = await openDb(
                ctx,
                topology.appId,
                topology.coreUrl,
                topology.adminSecret,
                "music-agent-direct-core-reader",
                secret,
              );
              conversation = await new JazzMusicStore(writer).createConversation("Edge read");
              await writer
                .insert(app.conversations, {
                  title: "direct writer barrier",
                  created_at: new Date(),
                })
                .wait({ tier: "global" });
              await reader
                .insert(app.conversations, {
                  title: "direct reader barrier",
                  created_at: new Date(),
                })
                .wait({ tier: "global" });
              expect(
                await reader.all(app.turns.where({ conversation_id: conversation }), {
                  tier: "edge",
                }),
              ).toEqual([]);
            },
            faultsAfter: [{ kind: "disconnect", target: "writerEdge" }],
          },
          {
            name: "write streamed transcript while the writer edge is blocked",
            run: async () => {
              await new DeterministicMusicAgent!(new JazzMusicStore!(writer!)).answer(
                conversation,
                "night drive",
              );
            },
            faultsAfter: [{ kind: "reconnect", target: "writerEdge" }],
          },
          {
            name: "flush writer edge and directly read logical text from the core",
            run: async () => {
              await writer!.all(app!.turns.where({ conversation_id: conversation }), {
                tier: "global",
              });
              let rows: Awaited<ReturnType<Db["all"]>> = [];
              await waitForCondition(
                async () => {
                  rows = await reader!.all(app!.turns.where({ conversation_id: conversation }), {
                    tier: "edge",
                  });
                  return rows.length === 3;
                },
                15_000,
                "core reader did not receive the direct-read transcript",
              );
              expect(rows.map((row) => row.role)).toEqual(["user", "assistant", "tool"]);
              expect(rows[1]?.body).toContain("night drive");
            },
          },
        ],
        cleanup: async () => {
          if (topology) await unblockJazzServerNetwork(topology.edgeUrl);
          await ctx.cleanup();
        },
      },
      browserTopologyReporter,
    );
  }, 60_000);
});

async function loadMusicAgentModules(): Promise<MusicAgentModules> {
  const [schemaModule, musicAgentModule] = await Promise.all([
    import("../../../../examples/music-agent/apps/ts-localfirst/schema.js"),
    import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"),
  ]);
  return {
    app: schemaModule.app,
    DeterministicMusicAgent: musicAgentModule.DeterministicMusicAgent,
    JazzMusicStore: musicAgentModule.JazzMusicStore,
  };
}

function defineMusicAgentPermissions(app: MusicAgentModules["app"]): CompiledPermissions {
  return schema.definePermissions(app, ({ policy }) => [
    policy.conversations.allowRead.always(),
    policy.conversations.allowInsert.always(),
    policy.turns.allowRead.always(),
    policy.turns.allowInsert.always(),
    policy.tool_calls.allowRead.always(),
    policy.tool_calls.allowInsert.always(),
    policy.attachments.allowRead.always(),
    policy.attachments.allowInsert.always(),
  ]);
}

async function openDb(
  ctx: TestCleanup,
  appId: string,
  serverUrl: string,
  adminSecret: string,
  label: string,
  secret: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      adminSecret,
      secret,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

function topologySeed(): number {
  const value = Number(__JAZZ_EXAMPLE_TOPOLOGY_SEED__);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("JAZZ_EXAMPLE_TOPOLOGY_SEED must be a non-negative safe integer");
  }
  return value;
}

function musicAgentReplay(seed: number, testName: string): string {
  return `JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed} pnpm --filter jazz-tools exec vitest run --config vitest.config.browser.ts tests/browser/music-agent.e2e.test.ts --testNamePattern=${JSON.stringify(testName)}`;
}
async function publish(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  appSchema: MusicAgentModules["app"]["wasmSchema"],
  permissions: CompiledPermissions,
): Promise<void> {
  const { hash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: appSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash: hash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });
}
