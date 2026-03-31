import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { createNapiRuntime, hasJazzNapiBuild } from "./testing/napi-runtime-test-utils.js";
import { SyncTracer, type TracableRuntime } from "./sync-tracer.js";

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

describe.skipIf(!hasJazzNapiBuild())("SyncTracer (NAPI)", () => {
  /**
   * A single runtime with tracing enabled records outgoing messages.
   * addServer() + insert triggers catalogue sync (ObjectUpdated).
   */
  it("captures outgoing sync messages from a single runtime", async () => {
    const tracer = new SyncTracer();
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-single-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", runtime as unknown as TracableRuntime);

    // addServer triggers catalogue sync → outgoing ObjectUpdateds
    runtime.addServer();
    runtime.onSyncMessageToSend(() => {});

    // Insert a row → triggers another outgoing ObjectUpdated
    runtime.insert("todos", {
      title: { type: "Text", value: "traced" },
      done: { type: "Boolean", value: false },
    });

    // Wait for the runtime to flush outbox
    await vi.waitFor(
      () => {
        expect(tracer.count()).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    const messages = tracer.messages();
    expect(messages.length).toBeGreaterThan(0);

    // All outgoing messages should be from "alice"
    const aliceMessages = tracer.from("alice");
    expect(aliceMessages.length).toBeGreaterThan(0);

    // Should have sent ObjectUpdated
    expect(tracer.ofType("ObjectUpdated").length).toBeGreaterThan(0);

    // The tally should mention "alice" and "server"
    const tally = tracer.tally();
    expect(tally).toContain("alice");
    expect(tally).toContain("server");

    console.log("=== Single runtime tally ===");
    console.log(tally);
  }, 15_000);

  /**
   * Outgoing messages are marked with side="send" and show => in summary.
   */
  it("marks outgoing messages with => arrow", async () => {
    const tracer = new SyncTracer();
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-arrows-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", runtime as unknown as TracableRuntime);
    runtime.addServer();
    runtime.onSyncMessageToSend(() => {});
    runtime.insert("todos", {
      title: { type: "Text", value: "arrows" },
      done: { type: "Boolean", value: false },
    });

    await vi.waitFor(
      () => {
        expect(tracer.count()).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    // Outgoing messages from alice should be "send" side
    for (const msg of tracer.from("alice")) {
      expect(msg.side).toBe("send");
    }

    // Summary should use => for outgoing
    const summary = tracer.summary();
    expect(summary).toContain("=>");

    console.log("=== Arrow summary ===");
    console.log(summary);
  }, 15_000);

  /**
   * clear() resets the message log across all runtimes.
   */
  it("clear() removes all messages", async () => {
    const tracer = new SyncTracer();
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-clear-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", runtime as unknown as TracableRuntime);
    runtime.addServer();
    runtime.onSyncMessageToSend(() => {});
    runtime.insert("todos", {
      title: { type: "Text", value: "clearable" },
      done: { type: "Boolean", value: false },
    });

    await vi.waitFor(
      () => {
        expect(tracer.count()).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    expect(tracer.count()).toBeGreaterThan(0);

    tracer.clear();

    expect(tracer.count()).toBe(0);
    expect(tracer.messages()).toHaveLength(0);
    expect(tracer.tally()).toBe("");
  }, 15_000);

  /**
   * Two runtimes with different names produce a merged tally
   * where both participants are visible.
   */
  it("aggregates messages from multiple runtimes", async () => {
    const tracer = new SyncTracer();

    const aliceRuntime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-multi-${randomUUID()}`,
      tier: "worker",
    });
    const bobRuntime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-multi-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", aliceRuntime as unknown as TracableRuntime);
    tracer.addRuntime("bob", bobRuntime as unknown as TracableRuntime);

    // Both runtimes trigger sync messages
    aliceRuntime.addServer();
    aliceRuntime.onSyncMessageToSend(() => {});
    aliceRuntime.insert("todos", {
      title: { type: "Text", value: "from-alice" },
      done: { type: "Boolean", value: false },
    });

    bobRuntime.addServer();
    bobRuntime.onSyncMessageToSend(() => {});
    bobRuntime.insert("todos", {
      title: { type: "Text", value: "from-bob" },
      done: { type: "Boolean", value: true },
    });

    await vi.waitFor(
      () => {
        expect(tracer.from("alice").length).toBeGreaterThan(0);
        expect(tracer.from("bob").length).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    const tally = tracer.tally();
    expect(tally).toContain("alice");
    expect(tally).toContain("bob");

    console.log("=== Multi-runtime tally ===");
    console.log(tally);
  }, 15_000);

  /**
   * Query helpers: from(), to(), between(), ofType() filter correctly.
   */
  it("query helpers filter messages", async () => {
    const tracer = new SyncTracer();
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-query-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", runtime as unknown as TracableRuntime);
    runtime.addServer();
    runtime.onSyncMessageToSend(() => {});
    runtime.insert("todos", {
      title: { type: "Text", value: "queryable" },
      done: { type: "Boolean", value: false },
    });

    await vi.waitFor(
      () => {
        expect(tracer.count()).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    // from("alice") should match all outgoing
    expect(tracer.from("alice").length).toBe(
      tracer.messages().filter((m) => m.from === "alice").length,
    );

    // to("server") should match messages going to server
    expect(tracer.to("server").length).toBeGreaterThan(0);

    // between("alice", "server") covers everything (only two participants)
    expect(tracer.between("alice", "server").length).toBe(tracer.count());

    // ofType filters by payload type
    const objectUpdates = tracer.ofType("ObjectUpdated");
    for (const msg of objectUpdates) {
      expect(msg.payload_type).toBe("ObjectUpdated");
    }
  }, 15_000);

  /**
   * registerObject() makes the per-runtime dump show human-readable object names.
   */
  it("registerObject names objects in the trace", async () => {
    const tracer = new SyncTracer();
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `tracer-named-${randomUUID()}`,
      tier: "worker",
    });

    tracer.addRuntime("alice", runtime as unknown as TracableRuntime);
    runtime.addServer();
    runtime.onSyncMessageToSend(() => {});

    const row = runtime.insert("todos", {
      title: { type: "Text", value: "buy milk" },
      done: { type: "Boolean", value: false },
    });

    // Register the object with a human name
    tracer.registerObject(row.id, "buy-milk");

    await vi.waitFor(
      () => {
        expect(tracer.count()).toBeGreaterThan(0);
      },
      { timeout: 5_000 },
    );

    // The per-runtime dump should show the named object
    const dump = (runtime as unknown as TracableRuntime).syncTracerDump();
    expect(dump).toBeDefined();
    expect(dump).toContain("buy-milk");

    // The per-runtime trace_normalized should also show it
    const normalized = (runtime as unknown as TracableRuntime).syncTracerTraceNormalized();
    expect(normalized).toBeDefined();
    expect(normalized).toContain("buy-milk");

    console.log("=== Named object dump ===");
    console.log(dump);
  }, 15_000);
});
