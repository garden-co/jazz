import { afterEach, describe, expect, it, vi } from "vitest";
import { createProjection } from "../../server/projection.js";
import type { Operation } from "../../shared/pending-operations.js";
import { app } from "../../schema.js";

const settledWrite = () => ({ wait: vi.fn(async () => undefined) });

afterEach(() => {
  vi.useRealTimers();
});

describe("profile projection", () => {
  it("does not write an unchanged profile twice", async () => {
    let storedProfile: Record<string, unknown> | null = null;
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => storedProfile),
      upsert: vi.fn((_table, data: Record<string, unknown>, options: { id: string }) => {
        storedProfile = { id: options.id, ...data };
        return settledWrite();
      }),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    const writer = createProjection(database);
    const profile = {
      did: "did:plc:viewer",
      handle: "viewer.test",
      indexedAt: "2026-07-17T08:00:00.000Z",
    };

    await writer.projectProfile(profile);
    await writer.projectProfile(profile);

    expect(database.upsert).toHaveBeenCalledTimes(1);
    expect(database.update).not.toHaveBeenCalled();
  });

  it("uses local acceptance so an unavailable edge cannot block later projections", async () => {
    const wait = vi.fn(async () => undefined);
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => null),
      upsert: vi.fn(() => ({ wait })),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };

    await createProjection(database).projectProfile({
      did: "did:plc:viewer",
      handle: "viewer.test",
      indexedAt: "2026-07-17T08:00:00.000Z",
    });

    expect(wait).not.toHaveBeenCalled();
  });

  it("does not overwrite enrichment with missing fields from a sparse source", async () => {
    const existing = {
      id: "profile-id",
      did: "did:plc:author",
      handle: "author.test",
      displayName: "Author",
      description: null,
      avatar: null,
      indexedAt: "2026-07-16T11:00:00.000Z",
    };
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => existing),
      upsert: vi.fn(settledWrite),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };

    await createProjection(database).projectProfile({
      did: "did:plc:author",
      indexedAt: "2026-07-16T10:00:00.000Z",
    });

    expect(database.update).not.toHaveBeenCalled();
  });
});

describe("durable reaction projection", () => {
  it.each(["queued", "sent"] as const)(
    "preserves a %s intention across writer instances until AppView confirms it",
    async (state) => {
      const operation: Operation = {
        id: "00000000-0000-0000-0000-000000000001",
        ownerDid: "did:plc:viewer",
        kind: "like",
        rkey: "3mlike",
        state,
        createdAt: "2026-07-16T10:00:00.000Z",
        payload: {
          subjectUri: "at://author/app.bsky.feed.post/post",
          subjectCid: "bafypost",
          active: true,
          createdAt: "2026-07-16T10:00:00.000Z",
        },
      };
      const database = {
        all: vi
          .fn()
          .mockResolvedValueOnce([{ ...operation, payload: JSON.stringify(operation.payload) }])
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([{ ...operation, payload: JSON.stringify(operation.payload) }])
          .mockResolvedValue([]),
        one: vi.fn(async () => null),
        upsert: vi.fn(settledWrite),
        update: vi.fn(settledWrite),
        delete: vi.fn(settledWrite),
      };
      const writer = createProjection(database);
      const post = {
        uri: operation.payload.subjectUri,
        authorDid: "did:plc:author",
        authorProfileId: "profile-id",
        text: "post",
        createdAt: operation.createdAt,
        indexedAt: operation.createdAt,
        replyCount: 0,
        likeCount: 0,
        repostCount: 0,
        state: "synced" as const,
      };

      await writer.projectTimelinePage(
        operation.ownerDid,
        [
          {
            post: {
              uri: post.uri,
              cid: "bafypost",
              author: { did: post.authorDid, handle: "author.test" },
              record: { text: post.text, createdAt: post.createdAt },
              indexedAt: post.indexedAt,
            },
          },
        ],
        "next",
      );
      expect(
        database.delete.mock.calls.filter(([table]) => table === app.pendingOperations),
      ).toHaveLength(0);
      expect(database.upsert.mock.calls.filter(([table]) => table === app.likes)).toHaveLength(0);

      const restartedWriter = createProjection(database);
      await restartedWriter.projectTimelinePage(
        operation.ownerDid,
        [
          {
            post: {
              uri: post.uri,
              cid: "bafypost",
              author: { did: post.authorDid, handle: "author.test" },
              record: { text: post.text, createdAt: post.createdAt },
              indexedAt: post.indexedAt,
              viewer: { like: "at://viewer/app.bsky.feed.like/3mlike" },
            },
          },
        ],
        "next",
      );
      expect(database.delete).toHaveBeenCalledWith(app.pendingOperations, operation.id);
      expect(database.upsert.mock.calls).toContainEqual([
        app.likes,
        expect.objectContaining({ active: true }),
        expect.anything(),
      ]);
    },
  );

  it("confirms one pending reaction once when a post appears twice", async () => {
    const subjectUri = "at://did:plc:author/app.bsky.feed.post/3m12345678921";
    const operation: Operation = {
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:viewer",
      kind: "like",
      rkey: "3mlike",
      state: "sent",
      createdAt: "2026-07-16T10:00:00.000Z",
      payload: {
        subjectUri,
        subjectCid: "bafypost",
        active: true,
        createdAt: "2026-07-16T10:00:00.000Z",
      },
    };
    const database = {
      all: vi
        .fn()
        .mockResolvedValueOnce([{ ...operation, payload: JSON.stringify(operation.payload) }])
        .mockResolvedValue([]),
      one: vi.fn(async () => null),
      upsert: vi.fn(settledWrite),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    const post = {
      uri: subjectUri,
      cid: "bafypost",
      author: { did: "did:plc:author", handle: "author.test" },
      record: { text: "Post", createdAt: operation.createdAt },
      indexedAt: operation.createdAt,
      viewer: { like: "at://did:plc:viewer/app.bsky.feed.like/3mlike" },
    };
    await createProjection(database).projectTimelinePage(
      operation.ownerDid,
      [{ post }, { post }],
      "next",
    );

    expect(database.delete).toHaveBeenCalledTimes(1);
  });
});

describe("projection API", () => {
  it("exposes only operations used by the bridge", async () => {
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => null),
      upsert: vi.fn(settledWrite),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    expect(Object.keys(createProjection(database)).sort()).toEqual([
      "projectPostOperation",
      "projectProfile",
      "projectReactionOperation",
      "projectThread",
      "projectTimelinePage",
    ]);
  });
});

describe("operation completion", () => {
  it("removes a completed post but retains a reaction until AppView confirms it", async () => {
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => null),
      upsert: vi.fn(settledWrite),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    const writer = createProjection(database);
    const post: Operation = {
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:viewer",
      kind: "post",
      rkey: "3mpost",
      state: "queued",
      createdAt: "2026-07-16T10:00:00.000Z",
      payload: { text: "Post", createdAt: "2026-07-16T10:00:00.000Z" },
    };
    const like: Operation = {
      id: "00000000-0000-0000-0000-000000000002",
      ownerDid: "did:plc:viewer",
      kind: "like",
      rkey: "3mlike",
      state: "queued",
      createdAt: post.createdAt,
      payload: {
        subjectUri: "at://did:plc:author/app.bsky.feed.post/3msubject",
        subjectCid: "bafysubject",
        active: true,
        createdAt: post.createdAt,
      },
    };

    const postView = {
      uri: `at://${post.ownerDid}/app.bsky.feed.post/${post.rkey}`,
      cid: "bafypost",
      author: { did: post.ownerDid, handle: "viewer.test" },
      record: post.payload,
      indexedAt: post.createdAt,
    };
    await writer.projectPostOperation(post, { uri: postView.uri, cid: postView.cid });
    await writer.projectReactionOperation(
      like,
      {
        ...postView,
        uri: like.payload.subjectUri,
        cid: like.payload.subjectCid,
      },
      { uri: `at://${like.ownerDid}/app.bsky.feed.like/${like.rkey}` },
    );

    expect(database.delete).toHaveBeenCalledWith(expect.anything(), post.id);
    expect(database.upsert).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ state: "sent" }),
      { id: like.id },
    );
  });
});

describe("thread projection", () => {
  it("does not rewrite an unchanged unavailable thread entry", async () => {
    let storedEntry: Record<string, unknown> | null = null;
    const database = {
      all: vi.fn(async () => []),
      one: vi.fn(async () => storedEntry),
      upsert: vi.fn((_table, data: Record<string, unknown>, options: { id: string }) => {
        storedEntry = { id: options.id, ...data };
        return settledWrite();
      }),
      update: vi.fn(settledWrite),
      delete: vi.fn(settledWrite),
    };
    const writer = createProjection(database);
    const uri = "at://did:plc:author/app.bsky.feed.post/missing";
    const thread = { uri, notFound: true };

    vi.setSystemTime("2026-07-16T10:00:00.000Z");
    await writer.projectThread("did:plc:viewer", uri, thread);
    vi.setSystemTime("2026-07-16T11:00:00.000Z");
    await writer.projectThread("did:plc:viewer", uri, thread);

    expect(database.update).not.toHaveBeenCalled();
  });
});

describe("progressive timeline projection", () => {
  it("does not let one slow feed item block the rest of the page", async () => {
    let releaseFirstLookup!: () => void;
    const firstLookup = new Promise<null>((resolve) => {
      releaseFirstLookup = () => resolve(null);
    });
    const database = {
      all: vi.fn(async () => []),
      one: vi
        .fn()
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

    const projection = createProjection(database).projectTimelinePage(
      "did:plc:viewer",
      [
        { post: post("did:plc:author1", "3m12345678921") },
        { post: post("did:plc:author2", "3m12345678922") },
      ],
      undefined,
    );

    try {
      await vi.waitFor(() => {
        expect(database.upsert).toHaveBeenCalledWith(
          expect.anything(),
          expect.objectContaining({
            uri: "at://did:plc:author2/app.bsky.feed.post/3m12345678922",
          }),
          expect.anything(),
        );
      });
    } finally {
      releaseFirstLookup();
      await projection;
    }
  });
});
