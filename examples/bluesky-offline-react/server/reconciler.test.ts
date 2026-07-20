import { describe, expect, it, vi } from "vitest";

vi.mock("./jazz.js", () => ({ db: {} }));

import { createReconciler } from "./reconciler.js";

function writer() {
  return {
    deactivateRepostTimelineEntries: vi.fn(async () => undefined),
    markOperationSent: vi.fn(),
    writeLike: vi.fn(),
    writePostBundle: vi.fn(async () => undefined),
    writeRepost: vi.fn(),
  };
}

describe("operation reconciliation", () => {
  it("preserves root and parent references in a queued reply", async () => {
    const putRecord = vi.fn(async () => ({
      uri: "at://did:plc:viewer/app.bsky.feed.post/3mreply",
      cid: "bafyreply",
    }));
    const root = { uri: "at://did:plc:author/app.bsky.feed.post/3mroot", cid: "bafyroot" };
    const parent = { uri: "at://did:plc:author/app.bsky.feed.post/3mparent", cid: "bafyparent" };
    const reconciler = createReconciler({
      deleteRecord: vi.fn(),
      fetchViewerPosts: vi.fn(),
      putRecord,
      recordKey: vi.fn(),
      writer: writer(),
    });

    await reconciler.reconcileOperations("did:plc:viewer", { fetchHandler: vi.fn() }, [{
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:viewer",
      kind: "post",
      rkey: "3mreply",
      state: "queued",
      createdAt: "2026-07-16T18:00:00.000Z",
      payload: {
        text: "A reply",
        createdAt: "2026-07-16T18:00:00.000Z",
        reply: { root, parent },
      },
    }]);

    expect(putRecord).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({
      record: expect.objectContaining({ reply: { root, parent } }),
    }));
  });

  it("reconciles intentions in queue order", async () => {
    let releaseFirst!: () => void;
    const firstWrite = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const putRecord = vi.fn(async (_session: unknown, request: { rkey: string }) => {
      if (request.rkey === "first") await firstWrite;
      return {
        uri: `at://did:plc:viewer/app.bsky.feed.post/${request.rkey}`,
        cid: `bafy-${request.rkey}`,
      };
    });
    const reconciler = createReconciler({
      deleteRecord: vi.fn(),
      fetchViewerPosts: vi.fn(),
      putRecord,
      recordKey: vi.fn(),
      writer: writer(),
    });
    const operation = (id: string, rkey: string) => ({
      id,
      ownerDid: "did:plc:viewer",
      kind: "post" as const,
      rkey,
      state: "queued" as const,
      createdAt: "2026-07-16T18:00:00.000Z",
      payload: { text: rkey, createdAt: "2026-07-16T18:00:00.000Z" },
    });

    const reconciliation = reconciler.reconcileOperations("did:plc:viewer", { fetchHandler: vi.fn() }, [
      operation("00000000-0000-0000-0000-000000000001", "first"),
      operation("00000000-0000-0000-0000-000000000002", "second"),
    ]);
    expect(putRecord).toHaveBeenCalledTimes(1);
    releaseFirst();
    await reconciliation;

    expect(putRecord.mock.calls.map(([, request]) => request.rkey)).toEqual(["first", "second"]);
  });
});
