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
import { getJazzServerInfo } from "./testing-server.js";
import { browserTopologyPhase } from "./topology-harness.js";

type MusicAgentModules = {
  app: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/schema.js"))["app"];
  DeterministicMusicAgent: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"))["DeterministicMusicAgent"];
  JazzMusicStore: (typeof import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"))["JazzMusicStore"];
};

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

describe("MusicAgent adopter E2E", () => {
  // #1844: streamed Text must cross the subscription/public binding as
  // logical text, never as an engine-owned indirect scalar descriptor.
  it("keeps an offline transcript local, then converges exactly once after reconnect", async () => {
    const { app, DeterministicMusicAgent, JazzMusicStore } = await loadMusicAgentModules();
    const permissions = defineMusicAgentPermissions(app);
    const { appId, serverUrl, adminSecret } = await browserTopologyPhase(
      "start topology core",
      () => getJazzServerInfo(uniqueDbName("music-agent-e2e")),
    );
    await browserTopologyPhase("publish topology schema and permissions", () =>
      publish(appId, serverUrl, adminSecret, app.wasmSchema, permissions),
    );
    const secret = generateAuthSecret();
    const writer = await browserTopologyPhase("open writer client", () =>
      openDb(appId, serverUrl, adminSecret, "music-agent-writer", secret),
    );
    const reader = await browserTopologyPhase("open reader client", () =>
      openDb(appId, serverUrl, adminSecret, "music-agent-reader", secret),
    );
    const writerStore = new JazzMusicStore(writer);
    const conversation = await browserTopologyPhase("writer creates conversation", () =>
      writerStore.createConversation("Offline listening"),
    );
    await browserTopologyPhase("writer initial edge connection", () =>
      writer
        .insert(app.conversations, { title: "connection barrier", created_at: new Date() })
        .wait({ tier: "edge" }),
    );
    await browserTopologyPhase("reader initial edge connection", () =>
      reader
        .insert(app.conversations, { title: "reader connection barrier", created_at: new Date() })
        .wait({ tier: "edge" }),
    );
    let receivedTranscriptSnapshot = false;
    let observedTranscript: Array<{ id: string; role: string }> = [];
    ctx.trackSubscription(
      reader.subscribeAll(app.turns.where({ conversation_id: conversation }), (delta) => {
        receivedTranscriptSnapshot = true;
        observedTranscript = delta.all.map((turn) => ({ id: turn.id, role: turn.role }));
      }),
    );
    await browserTopologyPhase("reader observes initial empty transcript", () =>
      waitForCondition(
        async () => receivedTranscriptSnapshot && observedTranscript.length === 0,
        10_000,
        "reader transcript subscription did not produce an empty initial snapshot",
      ),
    );
    await browserTopologyPhase("writer disconnect", () => writer.disconnect());
    expect(
      await browserTopologyPhase("write offline MusicAgent transcript", () =>
        new DeterministicMusicAgent(writerStore).answer(conversation, "wind-down piano"),
      ),
    ).toHaveLength(3);
    expect(observedTranscript).toEqual([]);
    await browserTopologyPhase("writer reconnect", () => writer.reconnect());
    await browserTopologyPhase("writer offline transcript flush", () =>
      writer.all(app.turns.where({ conversation_id: conversation }), { tier: "edge" }),
    );
    await browserTopologyPhase(
      "reader receives offline transcript",
      () =>
        waitForCondition(
          async () => observedTranscript.length === 3,
          15_000,
          "reader subscription did not receive offline MusicAgent transcript",
        ),
      20_000,
    );
    expect(observedTranscript.map((turn) => turn.role)).toEqual(["user", "assistant", "tool"]);
    expect(new Set(observedTranscript.map((turn) => turn.id)).size).toBe(3);
  }, 60_000);

  // #1844: retain the direct public read boundary separately from the
  // subscription scenario; either path must hydrate streamed Text.
  it("returns streamed transcript text from a reader edge query after reconnect", async () => {
    const { app, DeterministicMusicAgent, JazzMusicStore } = await loadMusicAgentModules();
    const permissions = defineMusicAgentPermissions(app);
    const { appId, serverUrl, adminSecret } = await browserTopologyPhase(
      "start direct-read topology core",
      () => getJazzServerInfo(uniqueDbName("music-agent-direct-read")),
    );
    await browserTopologyPhase("publish direct-read schema and permissions", () =>
      publish(appId, serverUrl, adminSecret, app.wasmSchema, permissions),
    );
    const secret = generateAuthSecret();
    const writer = await browserTopologyPhase("open direct-read writer", () =>
      openDb(appId, serverUrl, adminSecret, "music-agent-direct-writer", secret),
    );
    const reader = await browserTopologyPhase("open direct-read reader", () =>
      openDb(appId, serverUrl, adminSecret, "music-agent-direct-reader", secret),
    );
    const conversation = await new JazzMusicStore(writer).createConversation("Edge read");
    await browserTopologyPhase("connect direct-read writer", () =>
      writer
        .insert(app.conversations, { title: "direct writer barrier", created_at: new Date() })
        .wait({ tier: "edge" }),
    );
    await browserTopologyPhase("connect direct-read reader", () =>
      reader
        .insert(app.conversations, { title: "direct reader barrier", created_at: new Date() })
        .wait({ tier: "edge" }),
    );
    await browserTopologyPhase("disconnect direct-read writer", () => writer.disconnect());
    await browserTopologyPhase("write direct-read streamed transcript", () =>
      new DeterministicMusicAgent(new JazzMusicStore(writer)).answer(conversation, "night drive"),
    );
    await browserTopologyPhase("reconnect direct-read writer", () => writer.reconnect());
    await browserTopologyPhase("flush direct-read writer", () =>
      writer.all(app.turns.where({ conversation_id: conversation }), { tier: "edge" }),
    );
    const rows = await browserTopologyPhase(
      "reader direct edge transcript query",
      () => reader.all(app.turns.where({ conversation_id: conversation }), { tier: "edge" }),
      15_000,
    );
    expect(rows.map((row) => row.role)).toEqual(["user", "assistant", "tool"]);
    expect(rows[1]?.body).toContain("night drive");
  }, 60_000);
});

async function loadMusicAgentModules(): Promise<MusicAgentModules> {
  return browserTopologyPhase("load MusicAgent adopter modules", async () => {
    const [schemaModule, musicAgentModule] = await Promise.all([
      import("../../../../examples/music-agent/apps/ts-localfirst/schema.js"),
      import("../../../../examples/music-agent/apps/ts-localfirst/src/music-agent.js"),
    ]);
    return {
      app: schemaModule.app,
      DeterministicMusicAgent: musicAgentModule.DeterministicMusicAgent,
      JazzMusicStore: musicAgentModule.JazzMusicStore,
    };
  });
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
