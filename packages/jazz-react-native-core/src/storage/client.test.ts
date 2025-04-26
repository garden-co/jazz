import { beforeEach, describe, expect, it, vi } from "vitest";
import { SQLiteClient } from "./client.js";

describe("SQLiteClient.ensureInitialized", () => {
  let adapter: any;
  let client: SQLiteClient;

  beforeEach(() => {
    // a minimal adapter stub
    adapter = {
      initialize: vi.fn(),
      executeSync: vi.fn(),
      executeAsync: vi.fn(),
      transactionSync: vi.fn(),
      transactionAsync: vi.fn(),
    } as any;
    client = new SQLiteClient(adapter, {} as any); // second arg is OutgoingSyncQueue stub
  });

  it("calls initialize exactly once and then short-circuits", async () => {
    // simulate successful initialize()
    adapter.initialize.mockResolvedValueOnce(undefined);

    await client.ensureInitialized();
    expect(adapter.initialize).toHaveBeenCalledTimes(1);

    // second call should not re-invoke adapter.initialize
    await client.ensureInitialized();
    expect(adapter.initialize).toHaveBeenCalledTimes(1);
  });

  it("propagates errors, resets promise, and retries on next call", async () => {
    const err = new Error("boom");
    adapter.initialize
      .mockRejectedValueOnce(err)
      .mockResolvedValueOnce(undefined);

    // spy on console.error so you get feedback in your test app logs
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // first call fails
    await expect(client.ensureInitialized()).rejects.toThrowError("boom");
    expect(errorSpy).toHaveBeenCalledWith(
      "[SQLiteClient] ❌ initialization failed:",
      err,
    );

    // next call should attempt again
    await client.ensureInitialized();
    expect(adapter.initialize).toHaveBeenCalledTimes(2);

    errorSpy.mockRestore();
  });
});
