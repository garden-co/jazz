import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import type { PostOperation, ReactionOperation } from "../../shared/pending-operations.js";
import { stableObjectId } from "../../server/projection.js";

type BlueskyMocks = ReturnType<typeof bluesky>;
type WriterMocks = ReturnType<typeof projectionWriter>;

const moduleMocks = vi.hoisted(() => ({
  api: null as BlueskyMocks | null,
  writer: null as WriterMocks | null,
  OperationError: class OperationError extends Error {
    constructor(
      message: string,
      readonly status: 400 | 502 = 502,
    ) {
      super(message);
    }
  },
}));

function mockedApi() {
  if (!moduleMocks.api) throw new Error("Bluesky mocks are not configured");
  return moduleMocks.api;
}

vi.mock("../../server/bluesky.js", () => ({
  deleteRecord: (...args: Parameters<BlueskyMocks["deleteRecord"]>) =>
    mockedApi().deleteRecord(...args),
  fetchPostThread: (...args: Parameters<BlueskyMocks["fetchPostThread"]>) =>
    mockedApi().fetchPostThread(...args),
  fetchProfile: (...args: Parameters<BlueskyMocks["fetchProfile"]>) =>
    mockedApi().fetchProfile(...args),
  fetchTimelineFeed: (...args: Parameters<BlueskyMocks["fetchTimelineFeed"]>) =>
    mockedApi().fetchTimelineFeed(...args),
  fetchViewerPosts: (...args: Parameters<BlueskyMocks["fetchViewerPosts"]>) =>
    mockedApi().fetchViewerPosts(...args),
  OperationError: moduleMocks.OperationError,
  putRecord: (...args: Parameters<BlueskyMocks["putRecord"]>) => mockedApi().putRecord(...args),
  recordKey: (...args: Parameters<BlueskyMocks["recordKey"]>) => mockedApi().recordKey(...args),
}));

vi.mock("../../server/projection.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../server/projection.js")>();
  return {
    ...actual,
    createProjection: () => {
      if (!moduleMocks.writer) throw new Error("Projection mock is not configured");
      return moduleMocks.writer;
    },
  };
});

vi.mock("../../server/jazz.js", () => ({ projectionDb: {} }));

function projectionWriter() {
  return {
    projectPostOperation: vi.fn(async () => undefined),
    projectProfile: vi.fn(async () => undefined),
    projectReactionOperation: vi.fn(async () => undefined),
    projectThread: vi.fn(async () => ({ rootPostId: "root", count: 0 })),
    projectTimelinePage: vi.fn(async () => undefined),
  };
}

function bluesky(overrides: Partial<BlueskyMocks> = {}) {
  return {
    deleteRecord: vi.fn(async () => undefined),
    fetchPostThread: vi.fn(),
    fetchProfile: vi.fn(async () => undefined),
    fetchTimelineFeed: vi.fn(async () => ({ feed: [], cursor: "next" })),
    fetchViewerPosts: vi.fn(async () => []),
    putRecord: vi.fn(async (_session: unknown, request: { rkey: string }) => ({
      uri: `at://did:plc:viewer/app.bsky.feed.post/${request.rkey}`,
      cid: `bafy-${request.rkey}`,
    })),
    recordKey: vi.fn(() => "record-key"),
    ...overrides,
  };
}

async function loadBridge(api = bluesky(), writer = projectionWriter()) {
  vi.resetModules();
  moduleMocks.api = api;
  moduleMocks.writer = writer;
  return { api, writer, bridge: await import("../../server/bridge.js") };
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
    id: `00000000-0000-0000-0000-00000000000${kind === "like" ? "1" : "2"}`,
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

afterEach(() => {
  moduleMocks.api = null;
  moduleMocks.writer = null;
  vi.restoreAllMocks();
  vi.resetModules();
});

describe("ATProto → Jazz", () => {
  it("projects an AppView timeline into Jazz", async () => {
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-timeline-projection-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
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
    const { createProjection } = await vi.importActual<typeof import("../../server/projection.js")>(
      "../../server/projection.js",
    );
    moduleMocks.writer = createProjection(database);
    moduleMocks.api = bluesky({
      fetchProfile: vi.fn(async () => ({
        did: viewerDid,
        handle: "viewer.test",
        indexedAt: post.indexedAt,
      })),
      fetchTimelineFeed: vi.fn(async () => ({ feed: [{ post }], cursor: "next" })),
    });

    try {
      const { projectTimelinePage } = await import("../../server/bridge.js");
      await projectTimelinePage(viewerDid, { fetchHandler: vi.fn() });

      expect(
        await database.one(
          app.profiles.where({ id: { eq: stableObjectId("bluesky-profile", authorDid) } }),
        ),
      ).toMatchObject({ handle: "author.test" });
      expect(
        await database.one(
          app.posts.where({ id: { eq: stableObjectId("bluesky-post", post.uri) } }),
        ),
      ).toMatchObject({ text: "Hello" });
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });
});

describe("Jazz → ATProto", () => {
  it("writes queued posts to the PDS in order and preserves reply references", async () => {
    const root = { uri: "at://did:plc:author/app.bsky.feed.post/3mroot", cid: "bafyroot" };
    const parent = { uri: "at://did:plc:author/app.bsky.feed.post/3mparent", cid: "bafyparent" };
    const later = postOperation("2", "2026-07-16T18:00:01.000Z");
    const earlier = postOperation("1", "2026-07-16T18:00:00.000Z");
    earlier.payload.reply = { root, parent };
    const { api, writer, bridge } = await loadBridge();

    await bridge.reconcileOperations(earlier.ownerDid, { fetchHandler: vi.fn() }, [later, earlier]);

    expect(api.putRecord.mock.calls.map(([, request]) => request.rkey)).toEqual(["1", "2"]);
    expect(api.putRecord).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ record: expect.objectContaining({ reply: { root, parent } }) }),
    );
    expect(writer.projectPostOperation).toHaveBeenCalledTimes(2);
  });

  it("leaves a failed PDS write queued in Jazz", async () => {
    const error = new moduleMocks.OperationError("PDS write failed");
    const api = bluesky({
      putRecord: vi.fn(async () => {
        throw error;
      }),
    });
    const { bridge, writer } = await loadBridge(api);
    const operation = postOperation("3mpost", "2026-07-16T18:00:00.000Z");

    await expect(
      bridge.reconcileOperations(operation.ownerDid, { fetchHandler: vi.fn() }, [operation]),
    ).rejects.toBe(error);
    expect(writer.projectPostOperation).not.toHaveBeenCalled();
  });

  it("makes reaction intentions idempotent", async () => {
    const like = reactionOperation("like", true);
    const repost = reactionOperation("repost", false);
    const api = bluesky({
      fetchViewerPosts: vi
        .fn()
        .mockResolvedValueOnce([
          {
            uri: like.payload.subjectUri,
            cid: "bafy-current-version",
            author: { did: "did:plc:author", handle: "author.test" },
            record: { text: "Post", createdAt: like.createdAt },
            indexedAt: like.createdAt,
            viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
          },
        ])
        .mockResolvedValueOnce([
          {
            uri: repost.payload.subjectUri,
            cid: "bafy-current-version",
            author: { did: "did:plc:author", handle: "author.test" },
            record: { text: "Post", createdAt: repost.createdAt },
            indexedAt: repost.createdAt,
            viewer: { repost: "at://did:plc:viewer/app.bsky.feed.repost/3mrepost" },
          },
        ]),
    });
    const { bridge, writer } = await loadBridge(api);

    await bridge.reconcileOperations(like.ownerDid, { fetchHandler: vi.fn() }, [like, repost]);

    expect(api.putRecord).not.toHaveBeenCalled();
    expect(api.deleteRecord).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ collection: "app.bsky.feed.repost", rkey: "record-key" }),
    );
    expect(writer.projectReactionOperation).toHaveBeenCalledTimes(2);
  });
});
