import { describe, expect, test } from "vitest";
import { makeClient, testBatchId } from "./support.js";

describe("large-value client surface", () => {
  test("forwards logical cell coordinates and wraps edit transactions", async () => {
    const { client, largeValueCalls } = makeClient();
    const row = "00000000-0000-0000-0000-00000000004f";

    await expect(client.readValueRange("notes", row, "body", 2, 5)).resolves.toEqual(
      new Uint8Array([1, 2, 3]),
    );
    await expect(client.readTextUtf16Range("notes", row, "body", 6, 8)).resolves.toBe("🙂");
    await expect(client.readJsonPointer("notes", row, "body", "/selected")).resolves.toEqual({
      selected: true,
    });
    const append = await client.appendValue("notes", row, "body", new Uint8Array([4]));
    const splice = await client.spliceValue("notes", row, "body", 9, 3, new Uint8Array([5, 6]));
    await expect(append.batchId).resolves.toBe(testBatchId("append-transaction"));
    await expect(splice.batchId).resolves.toBe(testBatchId("splice-transaction"));

    expect(largeValueCalls).toEqual([
      ["range", "notes", row, "body", 2, 5],
      ["text", "notes", row, "body", 6, 8],
      ["json", "notes", row, "body", "/selected"],
      ["append", "notes", row, "body", new Uint8Array([4])],
      ["splice", "notes", row, "body", 9, 3, new Uint8Array([5, 6])],
    ]);
  });
});
