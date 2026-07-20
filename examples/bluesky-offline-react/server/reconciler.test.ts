import { describe, expect, it, vi } from "vitest";
import type { PostOperation, ReactionOperation } from "../operations.js";
import { OperationError } from "./bluesky.js";

vi.mock("./jazz.js", () => ({ db: {} }));

import { createReconciler } from "./reconciler.js";

function writer() {
  return {
    completeOperation: vi.fn(async () => undefined),
    deactivateRepostTimelineEntries: vi.fn(async () => undefined),
    writeLike: vi.fn(async () => undefined),
    writePostBundle: vi.fn(async () => undefined),
    writeRepost: vi.fn(async () => undefined),
  };
}

function postOperation(rkey: string, createdAt: string): PostOperation {
  return {
    id: `00000000-0000-0000-0000-${rkey.padStart(12, "0")}`,
    ownerDid: "did:plc:viewer",
    kind: "post",
    rkey,
    state: "queued",
    createdAt,
    payload: { text: rkey, createdAt },
  };
}

function reactionOperation(kind: "like" | "repost", active: boolean): ReactionOperation {
  return {
    id: "00000000-0000-0000-0000-000000000001",
    ownerDid: "did:plc:viewer",
    kind,
    rkey: `3m${kind}`,
    state: "queued",
    createdAt: "2026-07-16T18:00:00.000Z",
    payload: {
      subjectUri: "at://did:plc:author/app.bsky.feed.post/3mpost",
      subjectCid: "bafy-queued-version",
      active,
      createdAt: "2026-07-16T18:00:00.000Z",
    },
  };
}

function dependencies(overrides: Record<string, unknown> = {}) {
  return {
    deleteRecord: vi.fn(async () => undefined),
    fetchViewerPosts: vi.fn(async () => []),
    putRecord: vi.fn(async (_session: unknown, request: { rkey: string }) => ({
      uri: `at://did:plc:viewer/app.bsky.feed.post/${request.rkey}`,
      cid: `bafy-${request.rkey}`,
    })),
    recordKey: vi.fn(() => "record-key"),
    writer: writer(),
    ...overrides,
  };
}

describe("operation reconciliation", () => {
  it("preserves root and parent references in a queued reply", async () => {
    const root = { uri: "at://did:plc:author/app.bsky.feed.post/3mroot", cid: "bafyroot" };
    const parent = { uri: "at://did:plc:author/app.bsky.feed.post/3mparent", cid: "bafyparent" };
    const operation = postOperation("3mreply", "2026-07-16T18:00:00.000Z");
    operation.payload.reply = { root, parent };
    const deps = dependencies();

    await createReconciler(deps).reconcileOperations(
      operation.ownerDid,
      { fetchHandler: vi.fn() },
      [operation],
    );

    expect(deps.putRecord).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      record: expect.objectContaining({ reply: { root, parent } }),
    }));
    expect(deps.writer.completeOperation).toHaveBeenCalledWith(operation);
  });

  it("reconciles operations chronologically rather than trusting request order", async () => {
    const deps = dependencies();
    const later = postOperation("2", "2026-07-16T18:00:01.000Z");
    const earlier = postOperation("1", "2026-07-16T18:00:00.000Z");

    await createReconciler(deps).reconcileOperations(
      earlier.ownerDid,
      { fetchHandler: vi.fn() },
      [later, earlier],
    );

    expect(deps.putRecord.mock.calls.map(([, request]) => request.rkey)).toEqual(["1", "2"]);
  });

  it("does not complete an operation when its PDS write fails", async () => {
    const error = new OperationError("PDS write failed", 502);
    const deps = dependencies({ putRecord: vi.fn(async () => { throw error; }) });
    const operation = postOperation("3mpost", "2026-07-16T18:00:00.000Z");

    await expect(createReconciler(deps).reconcileOperations(
      operation.ownerDid,
      { fetchHandler: vi.fn() },
      [operation],
    )).rejects.toBe(error);

    expect(deps.writer.completeOperation).not.toHaveBeenCalled();
  });

  it("does not write another reaction record when AppView already matches the intention", async () => {
    const operation = reactionOperation("like", true);
    const deps = dependencies({
      fetchViewerPosts: vi.fn(async () => [{
        uri: operation.payload.subjectUri,
        cid: "bafy-current-version",
        author: { did: "did:plc:author", handle: "author.test" },
        record: { text: "Post", createdAt: operation.createdAt },
        indexedAt: operation.createdAt,
        viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
      }]),
    });

    await createReconciler(deps).reconcileOperations(
      operation.ownerDid,
      { fetchHandler: vi.fn() },
      [operation],
    );

    expect(deps.putRecord).not.toHaveBeenCalled();
    expect(deps.deleteRecord).not.toHaveBeenCalled();
    expect(deps.writer.writeLike).toHaveBeenCalledWith(expect.objectContaining({ active: true }));
  });

  it("deletes a repost and removes its timeline entries", async () => {
    const operation = reactionOperation("repost", false);
    const deps = dependencies({
      fetchViewerPosts: vi.fn(async () => [{
        uri: operation.payload.subjectUri,
        cid: "bafy-current-version",
        author: { did: "did:plc:author", handle: "author.test" },
        record: { text: "Post", createdAt: operation.createdAt },
        indexedAt: operation.createdAt,
        viewer: { repost: "at://did:plc:viewer/app.bsky.feed.repost/3mrepost" },
      }]),
    });

    await createReconciler(deps).reconcileOperations(
      operation.ownerDid,
      { fetchHandler: vi.fn() },
      [operation],
    );

    expect(deps.deleteRecord).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      collection: "app.bsky.feed.repost",
      rkey: "record-key",
    }));
    expect(deps.writer.writeRepost).toHaveBeenCalledWith(expect.objectContaining({ active: false }));
    expect(deps.writer.deactivateRepostTimelineEntries).toHaveBeenCalledOnce();
  });

  it("treats an already-missing reaction record as reconciled", async () => {
    const operation = reactionOperation("like", false);
    const deps = dependencies({
      deleteRecord: vi.fn(async () => {
        throw new OperationError("PDS com.atproto.repo.deleteRecord failed: RecordNotFound", 400);
      }),
      fetchViewerPosts: vi.fn(async () => [{
        uri: operation.payload.subjectUri,
        cid: "bafy-current-version",
        author: { did: "did:plc:author", handle: "author.test" },
        record: { text: "Post", createdAt: operation.createdAt },
        indexedAt: operation.createdAt,
        viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
      }]),
    });

    await expect(createReconciler(deps).reconcileOperations(
      operation.ownerDid,
      { fetchHandler: vi.fn() },
      [operation],
    )).resolves.toBeUndefined();

    expect(deps.writer.writeLike).toHaveBeenCalledWith(expect.objectContaining({ active: false }));
    expect(deps.writer.completeOperation).toHaveBeenCalledWith(operation);
  });
});
