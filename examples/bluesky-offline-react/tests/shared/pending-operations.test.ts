import { describe, expect, it } from "vitest";
import {
  decodeOperation,
  operationRow,
  parseOperationBatch,
} from "../../shared/pending-operations.js";

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

  it("decodes a reply intention from its Jazz row", () => {
    const reply = {
      root: { uri: "at://did:plc:alice/app.bsky.feed.post/3mroot", cid: "bafyroot" },
      parent: { uri: "at://did:plc:alice/app.bsky.feed.post/3mparent", cid: "bafyparent" },
    };

    expect(
      decodeOperation({
        ...post,
        payload: JSON.stringify({ text: "Hello", createdAt: post.createdAt, reply }),
      }),
    ).toMatchObject({
      kind: "post",
      payload: { text: "Hello", reply },
    });
  });

  it("decodes the final desired state of a reaction", () => {
    expect(
      decodeOperation({
        ...post,
        kind: "like",
        payload: JSON.stringify({
          subjectUri: "at://did:plc:bob/app.bsky.feed.post/3mpost",
          subjectCid: "bafypost",
          active: false,
          syncedActive: true,
          createdAt: post.createdAt,
        }),
      }),
    ).toMatchObject({
      kind: "like",
      payload: { active: false, syncedActive: true },
    });
  });

  it("rejects malformed or wrongly owned rows before reconciliation", () => {
    expect(() => decodeOperation({ ...post, payload: "{}" })).toThrow("Invalid post operation");
    expect(() => decodeOperation({ ...post, kind: "delete" })).toThrow(
      "Unsupported operation kind",
    );
    expect(parseOperationBatch([post], "did:plc:alice")).toHaveLength(1);
    expect(() => parseOperationBatch([post], "did:plc:bob")).toThrow("owner mismatch");
  });

  it("keeps the Jazz object ID out of pending-operation row data", () => {
    expect(operationRow(decodeOperation(post))).toEqual({
      ownerDid: "did:plc:alice",
      kind: "post",
      rkey: "3mreply",
      payload: post.payload,
      state: "queued",
      createdAt: "2026-07-16T18:00:00.000Z",
    });
  });
});
