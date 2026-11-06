import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { ThinConnectionLostError, ThinRuntime } from "./thin-runtime.js";

const schema: WasmSchema = {
  notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
};

describe("thin runtime close before schema binding", () => {
  it("rejects queued reads and write waits, including after repeated close", async () => {
    const runtime = new ThinRuntime(schema);
    const snapshot = runtime.captureSnapshot();
    const query = runtime.query(JSON.stringify({ table: "notes" }));
    const write = runtime.insert("notes", { text: { type: "Text", value: "queued" } });
    if (write.kind !== "committed") throw new Error("expected a standalone write");
    const wait = runtime.waitForTransaction(write.txId, "global");
    const settled = Promise.allSettled([snapshot, query, wait]);

    runtime.close();
    runtime.close();

    const outcomes = await settled;
    for (const outcome of outcomes) {
      expect(outcome.status).toBe("rejected");
      if (outcome.status === "rejected") {
        expect(outcome.reason).toBeInstanceOf(ThinConnectionLostError);
        expect(outcome.reason.message).toBe("the thin runtime is closed");
      }
    }
    await expect(runtime.captureSnapshot()).rejects.toThrow("the thin runtime is closed");
  });

  it("keeps unsent requests pending across disconnect until permanent close", async () => {
    const runtime = new ThinRuntime(schema);
    let outcome = "pending";
    const snapshot = runtime.captureSnapshot().then(
      () => {
        outcome = "resolved";
      },
      () => {
        outcome = "rejected";
      },
    );
    await runtime.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(outcome).toBe("pending");

    runtime.close();
    await snapshot;
    expect(outcome).toBe("rejected");
  });
});
