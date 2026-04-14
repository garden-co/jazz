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
    deleteWithSession: vi.fn(),
    executeSubscription: vi.fn(),
    flush: vi.fn(),
    getSchemaHash: vi.fn(() => "schema-hash"),
    insert: vi.fn((_table, _valuesJson) => JSON.stringify({ id: "row-1", values: [] })),
    insertWithSession: vi.fn((_table, _valuesJson, _writeContextJson) =>
      JSON.stringify({ id: "row-1", values: [] }),
    ),
    onBatchedTickNeeded: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    onSyncMessageReceivedFromClient: vi.fn(),
    query: vi.fn(() => JSON.stringify([{ id: "row-1", values: [] }])),
    removeServer: vi.fn(),
    setClientRole: vi.fn(),
    subscribe: vi.fn(() => 7n),
    unsubscribe: vi.fn(),
    update: vi.fn(),
    updateWithSession: vi.fn(),
    ...overrides,
  };
}

describe("JazzRnRuntimeAdapter", () => {
  it("defers batched tick execution to avoid re-entrancy", async () => {
    const binding = createBinding();
    new JazzRnRuntimeAdapter(binding, {});

    const onBatchedTickNeeded = binding.onBatchedTickNeeded as ReturnType<typeof vi.fn>;
    const callbackObject = onBatchedTickNeeded.mock.calls[0]![0];

    callbackObject.requestBatchedTick();
    expect(binding.batchedTick).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(binding.batchedTick).toHaveBeenCalledTimes(1);
  });

  it("serializes mutation payloads and parses query responses", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const row = adapter.insert("todos", { title: { type: "Text", value: "milk" } });
    expect(row).toEqual({ id: "row-1", values: [] });
    expect(binding.insert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
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

  it("serializes write context payloads for session-aware mutations", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});
    const writeContextJson = JSON.stringify({
      session: { user_id: "alice", claims: {} },
      attribution: "alice",
    });

    const row = adapter.insertWithSession(
      "todos",
      { title: { type: "Text", value: "milk" } },
      writeContextJson,
    );
    expect(row).toEqual({ id: "row-1", values: [] });
    expect(binding.insertWithSession).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      writeContextJson,
    );

    adapter.updateWithSession(
      "row-1",
      { done: { type: "Boolean", value: true } },
      writeContextJson,
    );
    expect(binding.updateWithSession).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      writeContextJson,
    );

    adapter.deleteWithSession("row-1", writeContextJson);
    expect(binding.deleteWithSession).toHaveBeenCalledWith("row-1", writeContextJson);

    await expect(
      adapter.insertDurableWithSession("todos", {}, writeContextJson, "worker"),
    ).resolves.toEqual({
      id: "row-1",
      values: [],
    });
    await expect(
      adapter.updateDurableWithSession("row-1", {}, writeContextJson, "worker"),
    ).resolves.toBeUndefined();
    await expect(
      adapter.deleteDurableWithSession("row-1", writeContextJson, "worker"),
    ).resolves.toBeUndefined();
    expect(binding.flush).toHaveBeenCalledTimes(3);
  });

  it("bridges subscription callbacks with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    const handle = adapter.subscribe("{}", onUpdate, null, null);
    expect(handle).toBe(7);

    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];
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
    expect(executeMock.mock.calls[0]![0]).toBe(9n);

    const callbackObject = executeMock.mock.calls[0]![1];
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

  it("swallows exceptions thrown by subscription callbacks crossing the native boundary", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn(() => {
      throw new Error("sub boom");
    });
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];
    expect(() => subscriptionCallback.onUpdate("[]")).not.toThrow();
  });

  it("passes canonical subscription tuple updates through unchanged", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];

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

    await expect(adapter.insertDurable("todos", {}, "worker")).resolves.toEqual({
      id: "row-1",
      values: [],
    });
    expect(binding.flush).toHaveBeenCalledTimes(1);

    await expect(adapter.updateDurable("row-1", {}, "worker")).resolves.toBeUndefined();
    await expect(adapter.deleteDurable("row-1", "worker")).resolves.toBeUndefined();
    expect(binding.flush).toHaveBeenCalledTimes(3);

    expect(() => adapter.insertDurable("todos", {}, "edge")).toThrow("supports only 'worker' tier");
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

  it("wraps Jazz RN errors with error name and cause", async () => {
    const runtimeError = {
      tag: "Runtime",
      inner: {
        message: "indexed value too large",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw runtimeError;
      }),
      query: vi.fn(() => {
        throw runtimeError;
      }),
      update: vi.fn(() => {
        throw runtimeError;
      }),
      delete_: vi.fn(() => {
        throw runtimeError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const insertError = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(insertError).toBeInstanceOf(Error);
    expect((insertError as Error).name).toBe("JazzRnRuntimeError");
    expect((insertError as Error).message).toBe("indexed value too large");
    expect((insertError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((insertError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const queryError = await adapter.query("{}", null, null).catch((error: unknown) => error);
    expect(queryError).toBeInstanceOf(Error);
    expect((queryError as Error).name).toBe("JazzRnRuntimeError");
    expect((queryError as Error).message).toBe("indexed value too large");
    expect((queryError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((queryError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const updateError = (() => {
      try {
        adapter.update("row-1", { done: true });
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(updateError).toBeInstanceOf(Error);
    expect((updateError as Error).name).toBe("JazzRnRuntimeError");
    expect((updateError as Error).message).toBe("indexed value too large");
    expect((updateError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((updateError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const deleteError = (() => {
      try {
        adapter.delete("row-1");
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(deleteError).toBeInstanceOf(Error);
    expect((deleteError as Error).name).toBe("JazzRnRuntimeError");
    expect((deleteError as Error).message).toBe("indexed value too large");
    expect((deleteError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((deleteError as Error & { tag?: unknown }).tag).toBe("Runtime");
  });

  it("does not wrap non-Jazz errors", () => {
    const binding = createBinding({
      insert: vi.fn(() => {
        throw new Error("plain failure");
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("Error");
    expect((error as Error).message).toBe("plain failure");
    expect((error as Error & { tag?: unknown }).tag).toBeUndefined();
  });

  it("derives error name for non-runtime Jazz tags", () => {
    const schemaError = {
      tag: "Schema",
      inner: {
        message: "schema mismatch",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw schemaError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("JazzRnSchemaError");
    expect((error as Error).message).toBe("schema mismatch");
    expect((error as Error & { tag?: unknown }).tag).toBe("Schema");
    expect((error as Error & { cause?: unknown }).cause).toBe(schemaError);
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
