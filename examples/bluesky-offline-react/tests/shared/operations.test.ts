import { describe, expect, it } from "vitest";
import { decodeOperation, parseOperationBatch } from "../../shared/operations.js";

describe("queued operations", () => {
  const post = {
    id: "00000000-0000-0000-0000-000000000001",
    ownerDid: "did:plc:alice",
    kind: "post",
    rkey: "3mreply",
    payload: JSON.stringify({ text: "Hello", createdAt: "2026-07-16T18:00:00.000Z" }),
    state: "queued",
    createdAt: "2026-07-16T18:00:00.000Z",
  };

  it("decodes the discriminated operation at the HTTP boundary", () => {
    expect(decodeOperation(post)).toMatchObject({
      kind: "post",
      payload: { text: "Hello" },
    });
  });

  it("rejects malformed payloads before they reach reconciliation", () => {
    expect(() => decodeOperation({ ...post, payload: "{}" })).toThrow("Invalid post operation");
    expect(() => decodeOperation({ ...post, kind: "delete" })).toThrow("Unsupported operation kind");
  });

  it("checks batch size and ownership once", () => {
    expect(parseOperationBatch([post], "did:plc:alice")).toHaveLength(1);
    expect(() => parseOperationBatch([post], "did:plc:bob")).toThrow("owner mismatch");
    expect(() => parseOperationBatch({}, "did:plc:alice")).toThrow("invalid operations");
  });
});
