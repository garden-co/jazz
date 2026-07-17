import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { stableObjectId } from "./timeline.js";

describe("timeline projection", () => {
  afterEach(() => {
    vi.doUnmock("./bluesky.js");
    vi.doUnmock("./jazz.js");
    vi.resetModules();
  });

  it("updates rows when the same deterministic objects are projected again", async () => {
    expect(stableObjectId("bluesky-profile", "did:plc:author", "app-v1"))
      .not.toBe(stableObjectId("bluesky-profile", "did:plc:author", "app-v2"));
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-timeline-projection-"));
    const dataPath = join(dataDirectory, "jazz.db");
    const appId = randomUUID();
    const context = createJazzContext({
      appId,
      app,
      permissions,
      driver: { type: "persistent", dataPath },
      env: "test",
      userBranch: "main",
    });
    const authorDid = "did:plc:author";
    const viewerDid = "did:plc:viewer";
    const database = context.db();
    const post = {
      uri: `at://${authorDid}/app.bsky.feed.post/3m12345678921`,
      cid: "bafypost",
      author: { did: authorDid, handle: "author.test" },
      record: { text: "Hello", createdAt: "2026-07-16T10:00:00.000Z" },
      indexedAt: "2026-07-16T10:00:01.000Z",
    };
    const viewerProfile = {
      did: viewerDid,
      handle: "viewer.test",
      indexedAt: "2026-07-16T10:00:01.000Z",
    };

    vi.doMock("./jazz.js", () => ({ db: database }));
    vi.doMock("./bluesky.js", () => ({
      deleteRecord: vi.fn(),
      fetchPostThread: vi.fn(),
      fetchProfile: vi.fn(async () => viewerProfile),
      fetchTimelineFeed: vi.fn(async () => ({
        feed: [
          { post },
          {
            post: {
              ...post,
              uri: `at://${authorDid}/app.bsky.feed.post/3m12345678922`,
              cid: "bafysecondpost",
              record: { ...post.record, text: "Second post" },
            },
          },
        ],
        cursor: "next",
      })),
      fetchViewerPosts: vi.fn(),
      OperationError: class OperationError extends Error {},
      putRecord: vi.fn(),
      recordKey: vi.fn(),
    }));
    const session = { fetchHandler: vi.fn() };

    try {
      const { getTimelineProjectionStatus, projectTimelinePage } = await import("./bridge.js");
      await projectTimelinePage(viewerDid, session);
      await vi.waitFor(async () => {
        expect(getTimelineProjectionStatus(viewerDid)?.state).toBe("completed");
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", authorDid) },
        }))).toMatchObject({ handle: "author.test" });
      });

      post.author.handle = "author-updated.test";
      viewerProfile.handle = "viewer-updated.test";
      await projectTimelinePage(viewerDid, session);

      await vi.waitFor(async () => {
        expect(getTimelineProjectionStatus(viewerDid)?.state).toBe("completed");
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", authorDid) },
        }))).toMatchObject({ handle: "author-updated.test" });
        expect(await database.one(app.profiles.where({
          id: { eq: stableObjectId("bluesky-profile", viewerDid) },
        }))).toMatchObject({ handle: "viewer-updated.test" });
      });
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("preserves root and parent references when reconciling a queued reply", async () => {
    const putRecord = vi.fn(async () => ({
      uri: "at://did:plc:viewer/app.bsky.feed.post/3mreply",
      cid: "bafyreply",
    }));
    const session = { fetchHandler: vi.fn() };
    const root = { uri: "at://did:plc:author/app.bsky.feed.post/3mroot", cid: "bafyroot" };
    const parent = { uri: "at://did:plc:author/app.bsky.feed.post/3mparent", cid: "bafyparent" };

    const { createReconciler } = await import("./reconciler.js");
    const writePostBundle = vi.fn(async () => undefined);
    const reconciler = createReconciler({
      deleteRecord: vi.fn(),
      fetchViewerPosts: vi.fn(),
      putRecord,
      recordKey: vi.fn(),
      writer: {
        deactivateRepostTimelineEntries: vi.fn(async () => undefined),
        markOperationSent: vi.fn(),
        writeLike: vi.fn(),
        writePostBundle,
        writeRepost: vi.fn(),
      },
    });
    await reconciler.reconcileOperations("did:plc:viewer", session, [{
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

    expect(putRecord).toHaveBeenCalledWith(session, expect.objectContaining({
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
    const { createReconciler } = await import("./reconciler.js");
    const reconciler = createReconciler({
      deleteRecord: vi.fn(),
      fetchViewerPosts: vi.fn(),
      putRecord,
      recordKey: vi.fn(),
      writer: {
        deactivateRepostTimelineEntries: vi.fn(async () => undefined),
        markOperationSent: vi.fn(),
        writeLike: vi.fn(),
        writePostBundle: vi.fn(async () => undefined),
        writeRepost: vi.fn(),
      },
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
