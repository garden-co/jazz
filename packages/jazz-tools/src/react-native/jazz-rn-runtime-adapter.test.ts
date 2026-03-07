import { describe, expect, it, vi } from "vitest";
import { JazzRnRuntimeAdapter, type JazzRnRuntimeBinding } from "./jazz-rn-runtime-adapter.js";

function createBinding(overrides: Partial<JazzRnRuntimeBinding> = {}): JazzRnRuntimeBinding {
  return {
    addClient: vi.fn(() => "client-1"),
    addServer: vi.fn(),
    batchedTick: vi.fn(),
    close: vi.fn(),
    createSubscription: vi.fn(() => 9n),
    delete_: vi.fn(),
    executeSubscription: vi.fn(),
    flush: vi.fn(),
    getSchemaHash: vi.fn(() => "schema-hash"),
    insert: vi.fn((_table, _valuesJson) => "row-1"),
    onBatchedTickNeeded: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    onSyncMessageReceivedFromClient: vi.fn(),
    onSyncMessageToSend: vi.fn(),
    query: vi.fn(() => JSON.stringify([{ id: "row-1", values: [] }])),
    removeServer: vi.fn(),
    setClientRole: vi.fn(),
    subscribe: vi.fn(() => 7n),
    unsubscribe: vi.fn(),
    update: vi.fn(),
    ...overrides,
  };
}

describe("JazzRnRuntimeAdapter", () => {
  it("defers batched tick execution to avoid re-entrancy", async () => {
    const binding = createBinding();
    new JazzRnRuntimeAdapter(binding, {});

    const onBatchedTickNeeded = binding.onBatchedTickNeeded as ReturnType<typeof vi.fn>;
    const callbackObject = onBatchedTickNeeded.mock.calls[0][0];

    callbackObject.requestBatchedTick();
    expect(binding.batchedTick).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(binding.batchedTick).toHaveBeenCalledTimes(1);
  });

  it("serializes mutation payloads and parses query responses", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const id = adapter.insert("todos", [{ type: "Text", value: "milk" }]);
    expect(id).toBe("row-1");
    expect(binding.insert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify([{ type: "Text", value: "milk" }]),
    );

    adapter.update("row-1", { done: { type: "Boolean", value: true } });
    expect(binding.update).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
    );

    adapter.delete("row-1");
    expect(binding.delete_).toHaveBeenCalledWith("row-1");

    await expect(adapter.query("{}", null, null)).resolves.toEqual([{ id: "row-1", values: [] }]);
  });

  it("bridges sync and subscription callbacks with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const syncHandler = vi.fn();
    adapter.onSyncMessageToSend(syncHandler);
    const onSyncMessageToSend = binding.onSyncMessageToSend as ReturnType<typeof vi.fn>;
    expect(onSyncMessageToSend).toHaveBeenCalledTimes(1);

    // Trigger callback captured by adapter wiring.
    const callbackObject = onSyncMessageToSend.mock.calls[0][0];
    callbackObject.onSyncMessage("server", "server-1", "{}", false);
    expect(syncHandler).toHaveBeenCalledWith("server", "server-1", "{}", false);

    const onUpdate = vi.fn();
    const handle = adapter.subscribe("{}", onUpdate, null, null);
    expect(handle).toBe(7);

    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0][1];
    subscriptionCallback.onUpdate('{"added":[],"removed":[],"updated":[],"pending":false}');
    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: false,
    });

    adapter.unsubscribe(handle);
    expect(binding.unsubscribe).toHaveBeenCalledWith(7n);
  });

  it("bridges 2-phase createSubscription + executeSubscription with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const handle = adapter.createSubscription("{}", null, null);
    expect(handle).toBe(9);
    expect(binding.createSubscription).toHaveBeenCalledWith("{}", undefined, undefined);

    const onUpdate = vi.fn();
    adapter.executeSubscription(handle, onUpdate);

    const executeMock = binding.executeSubscription as ReturnType<typeof vi.fn>;
    expect(executeMock).toHaveBeenCalledTimes(1);
    expect(executeMock.mock.calls[0][0]).toBe(9n);

    const callbackObject = executeMock.mock.calls[0][1];
    callbackObject.onUpdate('{"added":[],"removed":[],"updated":[],"pending":false}');
    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: false,
    });

    adapter.unsubscribe(handle);
    expect(binding.unsubscribe).toHaveBeenCalledWith(9n);
  });

  it("swallows exceptions thrown by JS callbacks crossing the native boundary", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.onSyncMessageToSend(() => {
      throw new Error("sync boom");
    });
    const onSyncMessageToSend = binding.onSyncMessageToSend as ReturnType<typeof vi.fn>;
    const syncCallback = onSyncMessageToSend.mock.calls[0][0];
    expect(() => syncCallback.onSyncMessage("server", "server-1", "{}", false)).not.toThrow();

    const onUpdate = vi.fn(() => {
      throw new Error("sub boom");
    });
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0][1];
    expect(() => subscriptionCallback.onUpdate("[]")).not.toThrow();
  });

  it("passes canonical subscription tuple updates through unchanged", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0][1];

    subscriptionCallback.onUpdate(
      JSON.stringify({
        added: [],
        removed: [],
        updated: [
          [
            { id: "row-u", values: [{ type: "Text", value: "before" }] },
            { id: "row-u", values: [{ type: "Text", value: "after" }] },
          ],
        ],
        pending: false,
      }),
    );

    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [
        [
          { id: "row-u", values: [{ type: "Text", value: "before" }] },
          { id: "row-u", values: [{ type: "Text", value: "after" }] },
        ],
      ],
      pending: false,
    });
  });

  it("supports worker-tier persisted mutations and rejects global tiers", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    await expect(adapter.insertDurable("todos", [], "worker")).resolves.toBe("row-1");
    expect(binding.flush).toHaveBeenCalledTimes(1);

    await expect(adapter.updateDurable("row-1", {}, "worker")).resolves.toBeUndefined();
    await expect(adapter.deleteDurable("row-1", "worker")).resolves.toBeUndefined();
    expect(binding.flush).toHaveBeenCalledTimes(3);

    expect(() => adapter.insertDurable("todos", [], "edge")).toThrow("supports only 'worker' tier");
  });

  it("swallows ObjectNotFound runtime errors for update/delete", () => {
    const objectNotFound = {
      tag: "Runtime",
      inner: {
        message: 'WriteError("ObjectNotFound(ObjectId(019c70f1-8514-72f0-bad8-8d849e1c3e70))")',
      },
    };
    const binding = createBinding({
      update: vi.fn(() => {
        throw objectNotFound;
      }),
      delete_: vi.fn(() => {
        throw objectNotFound;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    expect(() => adapter.update("row-1", { done: true })).not.toThrow();
    expect(() => adapter.delete("row-1")).not.toThrow();
  });

  it("no-ops sync hooks after close", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.close();
    adapter.addServer();
    adapter.removeServer();
    adapter.onSyncMessageReceived('{"Ping":{}}');
    adapter.onSyncMessageReceivedFromClient("client-1", '{"Ping":{}}');

    expect(binding.addServer).not.toHaveBeenCalled();
    expect(binding.removeServer).not.toHaveBeenCalled();
    expect(binding.onSyncMessageReceived).not.toHaveBeenCalled();
    expect(binding.onSyncMessageReceivedFromClient).not.toHaveBeenCalled();
  });
});
