import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import type { PostOperation, ReactionOperation } from "../../shared/pending-operations.js";
import permissions from "../../permissions.js";
import { stableObjectId } from "../../server/projection.js";
import { app } from "../../schema.js";

type BlueskyMocks = ReturnType<typeof bluesky>;
type WriterMocks = ReturnType<typeof projectionWriter>;

const moduleMocks = vi.hoisted(() => ({
  api: null as BlueskyMocks | null,
  writer: null as WriterMocks | null,
  createProjection: vi.fn(),
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
      moduleMocks.createProjection();
      if (!moduleMocks.writer) throw new Error("Projection mock is not configured");
      return moduleMocks.writer;
    },
  };
});

vi.mock("../../server/jazz.js", () => ({ projectionDb: {} }));

function projectionWriter(overrides: Record<string, unknown> = {}) {
  return {
    projectPostOperation: vi.fn(async () => undefined),
    projectProfile: vi.fn(async () => undefined),
    projectReactionOperation: vi.fn(async () => undefined),
    projectThread: vi.fn(async () => ({ rootPostId: "root", count: 0 })),
    projectTimelinePage: vi.fn(async () => undefined),
    ...overrides,
  };
}

function bluesky(overrides: Record<string, unknown> = {}) {
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

async function loadBridge(
  options: {
    api?: ReturnType<typeof bluesky>;
    writer?: ReturnType<typeof projectionWriter>;
  } = {},
) {
  vi.resetModules();
  const api = options.api ?? bluesky();
  const writer = options.writer ?? projectionWriter();
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

afterEach(() => {
  moduleMocks.api = null;
  moduleMocks.writer = null;
  moduleMocks.createProjection.mockClear();
  vi.restoreAllMocks();
  vi.resetModules();
});

describe("ATProto to Jazz projection", () => {
  it("projects an ATProto timeline into Jazz", async () => {
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

      await vi.waitFor(async () => {
        expect(
          await database.one(
            app.profiles.where({
              id: { eq: stableObjectId("bluesky-profile", authorDid) },
            }),
          ),
        ).toMatchObject({ handle: "author.test" });
        expect(
          await database.one(
            app.posts.where({
              id: { eq: stableObjectId("bluesky-post", post.uri) },
            }),
          ),
        ).toMatchObject({ text: "Hello" });
      });
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("uses one Jazz writer for projection and reconciliation", async () => {
    moduleMocks.api = bluesky();
    moduleMocks.writer = projectionWriter();

    await import("../../server/bridge.js");

    expect(moduleMocks.createProjection).toHaveBeenCalledOnce();
  });

  it("projects the signed-in profile without waiting for the timeline", async () => {
    const profile = { did: "did:plc:viewer", handle: "viewer.test" };
    const writer = projectionWriter();
    const { bridge } = await loadBridge({
      api: bluesky({
        fetchTimelineFeed: vi.fn(() => new Promise(() => undefined)),
        fetchProfile: vi.fn(async () => profile),
      }),
      writer,
    });

    bridge.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });

    await vi.waitFor(() => expect(writer.projectProfile).toHaveBeenCalledWith(profile));
  });

  it("shares an in-flight projection but keeps pagination and head refreshes separate", async () => {
    const releases = new Map<string, (value: { feed: never[]; cursor?: string }) => void>();
    const fetchTimelineFeed = vi.fn(
      (_session: unknown, cursor?: string) =>
        new Promise<{ feed: never[]; cursor?: string }>((resolve) => {
          releases.set(cursor ?? "head", resolve);
        }),
    );
    const { bridge } = await loadBridge({ api: bluesky({ fetchTimelineFeed }) });
    const session = { fetchHandler: vi.fn() };

    const firstHead = bridge.projectTimelinePage("did:plc:viewer", session);
    const secondHead = bridge.projectTimelinePage("did:plc:viewer", session);
    const pagination = bridge.projectTimelinePage("did:plc:viewer", session, "older");

    expect(fetchTimelineFeed).toHaveBeenCalledTimes(2);
    releases.get("head")?.({ feed: [], cursor: "newer" });
    releases.get("older")?.({ feed: [], cursor: "oldest" });
    await expect(Promise.all([firstHead, secondHead])).resolves.toEqual([
      { cursor: "newer", hasMore: true, count: 0 },
      { cursor: "newer", hasMore: true, count: 0 },
    ]);
    await expect(pagination).resolves.toEqual({ cursor: "oldest", hasMore: true, count: 0 });
  });

  it("reuses a recently completed head refresh", async () => {
    const fetchTimelineFeed = vi.fn(async () => ({ feed: [], cursor: "next" }));
    const { bridge, writer } = await loadBridge({ api: bluesky({ fetchTimelineFeed }) });
    const session = { fetchHandler: vi.fn() };

    const first = await bridge.projectTimelinePage("did:plc:viewer", session);
    const second = await bridge.projectTimelinePage("did:plc:viewer", session);

    expect(second).toEqual(first);
    expect(fetchTimelineFeed).toHaveBeenCalledOnce();
    expect(writer.projectTimelinePage).toHaveBeenCalledOnce();
  });

  it("does not report a refresh complete until Jazz has accepted the projection", async () => {
    let releaseProjection!: () => void;
    const projectionComplete = new Promise<void>((resolve) => {
      releaseProjection = resolve;
    });
    const { bridge } = await loadBridge({
      api: bluesky({
        fetchTimelineFeed: vi.fn(async () => ({ feed: [], cursor: "next" })),
      }),
      writer: projectionWriter({
        projectTimelinePage: vi.fn(() => projectionComplete),
      }),
    });
    let settled = false;

    const refresh = bridge.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() });
    refresh.then(() => {
      settled = true;
    });
    await vi.waitFor(() => expect(moduleMocks.writer?.projectTimelinePage).toHaveBeenCalledOnce());
    expect(settled).toBe(false);

    releaseProjection();
    await expect(refresh).resolves.toEqual({ cursor: "next", hasMore: true, count: 0 });
  });

  it("reports a projection failure to the refresh caller", async () => {
    const error = new Error("projection exploded");
    const { bridge } = await loadBridge({
      writer: projectionWriter({
        projectTimelinePage: vi.fn(async () => {
          throw error;
        }),
      }),
    });

    await expect(
      bridge.projectTimelinePage("did:plc:viewer", { fetchHandler: vi.fn() }),
    ).rejects.toBe(error);
  });
});

describe("Jazz outbox to ATProto reconciliation", () => {
  it("preserves reply references and applies operations chronologically", async () => {
    const root = { uri: "at://did:plc:author/app.bsky.feed.post/3mroot", cid: "bafyroot" };
    const parent = { uri: "at://did:plc:author/app.bsky.feed.post/3mparent", cid: "bafyparent" };
    const later = postOperation("2", "2026-07-16T18:00:01.000Z");
    const earlier = postOperation("1", "2026-07-16T18:00:00.000Z");
    earlier.payload.reply = { root, parent };
    const api = bluesky();
    const { bridge, writer } = await loadBridge({ api });

    await bridge.reconcileOperations(earlier.ownerDid, { fetchHandler: vi.fn() }, [later, earlier]);

    expect(api.putRecord.mock.calls.map(([, request]) => request.rkey)).toEqual(["1", "2"]);
    expect(api.putRecord).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        record: expect.objectContaining({ reply: { root, parent } }),
      }),
    );
    expect(writer.projectPostOperation).toHaveBeenCalledTimes(2);
  });

  it("does not complete an operation when its PDS write fails", async () => {
    const error = new moduleMocks.OperationError("PDS write failed");
    const writer = projectionWriter();
    const { bridge } = await loadBridge({
      api: bluesky({
        putRecord: vi.fn(async () => {
          throw error;
        }),
      }),
      writer,
    });
    const operation = postOperation("3mpost", "2026-07-16T18:00:00.000Z");

    await expect(
      bridge.reconcileOperations(operation.ownerDid, { fetchHandler: vi.fn() }, [operation]),
    ).rejects.toBe(error);
    expect(writer.projectPostOperation).not.toHaveBeenCalled();
  });

  it("does not create another reaction when AppView already matches the intention", async () => {
    const operation = reactionOperation("like", true);
    const api = bluesky({
      fetchViewerPosts: vi.fn(async () => [
        {
          uri: operation.payload.subjectUri,
          cid: "bafy-current-version",
          author: { did: "did:plc:author", handle: "author.test" },
          record: { text: "Post", createdAt: operation.createdAt },
          indexedAt: operation.createdAt,
          viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
        },
      ]),
    });
    const { bridge, writer } = await loadBridge({ api });

    await bridge.reconcileOperations(operation.ownerDid, { fetchHandler: vi.fn() }, [operation]);

    expect(api.putRecord).not.toHaveBeenCalled();
    expect(api.deleteRecord).not.toHaveBeenCalled();
    expect(writer.projectReactionOperation).toHaveBeenCalledWith(
      operation,
      expect.objectContaining({ viewer: { like: expect.any(String) } }),
      expect.objectContaining({ uri: expect.any(String) }),
    );
  });

  it("deletes a repost and deactivates its timeline entries", async () => {
    const operation = reactionOperation("repost", false);
    const api = bluesky({
      fetchViewerPosts: vi.fn(async () => [
        {
          uri: operation.payload.subjectUri,
          cid: "bafy-current-version",
          author: { did: "did:plc:author", handle: "author.test" },
          record: { text: "Post", createdAt: operation.createdAt },
          indexedAt: operation.createdAt,
          viewer: { repost: "at://did:plc:viewer/app.bsky.feed.repost/3mrepost" },
        },
      ]),
    });
    const { bridge, writer } = await loadBridge({ api });

    await bridge.reconcileOperations(operation.ownerDid, { fetchHandler: vi.fn() }, [operation]);

    expect(api.deleteRecord).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        collection: "app.bsky.feed.repost",
        rkey: "record-key",
      }),
    );
    expect(writer.projectReactionOperation).toHaveBeenCalledWith(operation, expect.anything(), {
      uri: undefined,
      cid: undefined,
    });
  });

  it("treats an already-missing reaction record as reconciled", async () => {
    const operation = reactionOperation("like", false);
    const writer = projectionWriter();
    const { bridge } = await loadBridge({
      api: bluesky({
        deleteRecord: vi.fn(async () => {
          throw new moduleMocks.OperationError("PDS delete failed: RecordNotFound", 400);
        }),
        fetchViewerPosts: vi.fn(async () => [
          {
            uri: operation.payload.subjectUri,
            cid: "bafy-current-version",
            author: { did: "did:plc:author", handle: "author.test" },
            record: { text: "Post", createdAt: operation.createdAt },
            indexedAt: operation.createdAt,
            viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
          },
        ]),
      }),
      writer,
    });

    await expect(
      bridge.reconcileOperations(operation.ownerDid, { fetchHandler: vi.fn() }, [operation]),
    ).resolves.toBeUndefined();
    expect(writer.projectReactionOperation).toHaveBeenCalledWith(operation, expect.anything(), {
      uri: undefined,
      cid: undefined,
    });
  });
});

it("exposes only the application operations", async () => {
  const { bridge } = await loadBridge();

  expect(Object.keys(bridge).sort()).toEqual([
    "projectThread",
    "projectTimelinePage",
    "reconcileOperations",
  ]);
});
