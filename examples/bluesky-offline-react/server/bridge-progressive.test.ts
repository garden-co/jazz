import { afterEach, describe, expect, it, vi } from "vitest";

describe("progressive timeline projection", () => {
  const settledWrite = () => ({ wait: vi.fn(async () => undefined) });

  afterEach(() => {
    vi.doUnmock("./bluesky.js");
    vi.doUnmock("./jazz.js");
    vi.resetModules();
  });

  it("does not let one slow feed item block the rest of the page", async () => {
    let releaseFirstLookup!: () => void;
    const firstLookup = new Promise<null>((resolve) => {
      releaseFirstLookup = () => resolve(null);
    });
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn()
        .mockImplementationOnce(() => firstLookup)
        .mockResolvedValue(null),
      upsert: vi.fn(settledWrite),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    const post = (did: string, rkey: string) => ({
      uri: `at://${did}/app.bsky.feed.post/${rkey}`,
      cid: `bafy-${rkey}`,
      author: { did, handle: `${did.slice(-1)}.test` },
      record: { text: `Post ${rkey}`, createdAt: "2026-07-16T10:00:00.000Z" },
      indexedAt: "2026-07-16T10:00:01.000Z",
    });

    vi.doMock("./jazz.js", () => ({ getBackendDb: () => database }));
    vi.doMock("./bluesky.js", () => ({
      deleteRecord: vi.fn(),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(),
      fetchTimelineFeed: vi.fn(async () => ({
        feed: [
          { post: post("did:plc:author1", "3m12345678921") },
          { post: post("did:plc:author2", "3m12345678922") },
        ],
      })),
      fetchViewerPosts: vi.fn(),
      OperationError: class OperationError extends Error {},
      putRecord: vi.fn(),
      recordKey: vi.fn(),
    }));
    const session = { fetchHandler: vi.fn() };

    try {
      const { projectTimelinePage } = await import("./bridge.js");
      await projectTimelinePage("did:plc:viewer", session);
      await vi.waitFor(() => {
        expect(database.upsert).toHaveBeenCalledWith(
          expect.anything(),
          expect.objectContaining({
            uri: "at://did:plc:author2/app.bsky.feed.post/3m12345678922",
          }),
          expect.anything(),
        );
      }, { timeout: 100 });
    } finally {
      releaseFirstLookup();
    }
  });
});
