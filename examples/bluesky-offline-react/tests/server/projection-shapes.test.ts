import type { AppBskyFeedDefs } from "@atproto/api";
import { describe, expect, it, vi } from "vitest";
import { createProjection, stableObjectId } from "../../server/projection.js";
import { app } from "../../schema.js";
import { operationRow, type ReactionOperation } from "../../shared/pending-operations.js";

const settledWrite = () => ({ wait: vi.fn(async () => undefined) });
const testCid = "bafkreie7q3iidccmpvszul7kudcvvuavuo7u6gzlbobczuk5nqk3b4akba";

function database() {
  return {
    all: vi.fn(async () => []),
    one: vi.fn(async () => null),
    upsert: vi.fn(settledWrite),
    update: vi.fn(settledWrite),
    delete: vi.fn(settledWrite),
  };
}

function post(uri: string, indexedAt = "2026-07-15T08:00:01.000Z"): AppBskyFeedDefs.PostView {
  return {
    $type: "app.bsky.feed.defs#postView",
    uri,
    cid: testCid,
    author: { did: "did:plc:author", handle: "author.test" },
    record: { text: "A post", createdAt: indexedAt },
    indexedAt,
  };
}

function likeOperation(): ReactionOperation {
  const createdAt = "2026-07-16T18:00:00.000Z";
  return {
    id: "00000000-0000-0000-0000-000000000001",
    ownerDid: "did:plc:viewer",
    kind: "like",
    rkey: "3mlike",
    state: "queued",
    createdAt,
    payload: {
      subjectUri: "at://did:plc:author/app.bsky.feed.post/3mpost",
      subjectCid: testCid,
      active: true,
      createdAt,
    },
  };
}

describe("post projection", () => {
  it("projects a quoted post alongside its outer post", async () => {
    const quotedUri = "at://did:plc:quoted/app.bsky.feed.post/3m12345678920";
    const db = database();
    await createProjection(db).projectTimelinePage(
      "did:plc:viewer",
      [
        {
          post: {
            ...post("at://did:plc:author/app.bsky.feed.post/3m12345678921"),
            record: { text: "Worth reading", createdAt: "2026-07-17T08:00:00.000Z" },
            embed: {
              $type: "app.bsky.embed.record#view",
              record: {
                $type: "app.bsky.embed.record#viewRecord",
                uri: quotedUri,
                cid: "bafyquoted",
                author: { did: "did:plc:quoted", handle: "quoted.test" },
                value: { text: "The quoted post", createdAt: "2026-07-17T07:00:00.000Z" },
                indexedAt: "2026-07-17T07:00:01.000Z",
              },
            },
          },
        },
      ],
      "next",
    );

    const projectedPosts = db.upsert.mock.calls
      .filter(([table]) => table === app.posts)
      .map(([, row]) => row);
    expect(projectedPosts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ uri: quotedUri, text: "The quoted post" }),
        expect.objectContaining({ quotedPostId: stableObjectId("bluesky-post", quotedUri) }),
      ]),
    );
  });

  it("projects images from record-with-media posts", async () => {
    const db = database();
    await createProjection(db).projectTimelinePage(
      "did:plc:viewer",
      [
        {
          post: {
            ...post("at://did:plc:author/app.bsky.feed.post/3m12345678922"),
            embed: {
              $type: "app.bsky.embed.recordWithMedia#view",
              media: {
                $type: "app.bsky.embed.images#view",
                images: [{ thumb: "https://cdn.test/thumb", fullsize: "https://cdn.test/full" }],
              },
              record: {
                $type: "app.bsky.embed.record#view",
                record: {
                  $type: "app.bsky.embed.record#viewRecord",
                  uri: "at://did:plc:quoted/app.bsky.feed.post/3m12345678920",
                  cid: "bafyquoted",
                  author: { did: "did:plc:quoted" },
                  value: { text: "Quoted", createdAt: "2026-07-17T07:00:00.000Z" },
                },
              },
            },
          },
        },
      ],
      "next",
    );

    expect(db.upsert.mock.calls).toContainEqual([
      app.postImages,
      expect.objectContaining({ fullsize: "https://cdn.test/full" }),
      expect.anything(),
    ]);
  });
});

describe("timeline projection", () => {
  it("leaves a matching queued reaction for the outbox to complete", async () => {
    const operation = likeOperation();
    const db = database();
    db.all.mockResolvedValueOnce([{ id: operation.id, ...operationRow(operation) }]);
    const likedPost = {
      ...post(operation.payload.subjectUri, operation.createdAt),
      viewer: { like: `at://${operation.ownerDid}/app.bsky.feed.like/${operation.rkey}` },
    };

    await createProjection(db).projectTimelinePage(
      operation.ownerDid,
      [{ post: likedPost }],
      "next",
    );

    expect(db.delete).not.toHaveBeenCalledWith(app.pendingOperations, operation.id);
  });

  it("anchors a reply thread to the root post time", async () => {
    const root = post(
      "at://did:plc:author/app.bsky.feed.post/3m12345678920",
      "2026-07-15T07:00:01.000Z",
    );
    const reply = post(
      "at://did:plc:author/app.bsky.feed.post/3m12345678921",
      "2026-07-16T10:00:01.000Z",
    );
    reply.record!.reply = {
      root: { uri: root.uri, cid: root.cid },
      parent: { uri: root.uri, cid: root.cid },
    };
    const db = database();

    await createProjection(db).projectTimelinePage(
      "did:plc:viewer",
      [
        {
          post: reply,
          reply: { root, parent: root },
        },
      ],
      "next",
    );

    expect(db.upsert.mock.calls).toContainEqual([
      app.timelineEntries,
      expect.objectContaining({ sortAt: root.indexedAt }),
      expect.anything(),
    ]);
  });

  it("keeps direct and repost events distinct", async () => {
    const sharedPost = post("at://did:plc:author/app.bsky.feed.post/3m12345678922");
    const db = database();
    await createProjection(db).projectTimelinePage(
      "did:plc:viewer",
      [
        { post: sharedPost },
        {
          post: sharedPost,
          reason: {
            $type: "app.bsky.feed.defs#reasonRepost",
            by: { did: "did:plc:reposter", handle: "reposter.test" },
            uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
            indexedAt: "2026-07-15T09:00:00.000Z",
          },
        },
      ],
      "next",
    );

    const timelineIds = db.upsert.mock.calls
      .filter(([table]) => table === app.timelineEntries)
      .map(([, , options]) => options.id);
    expect(new Set(timelineIds).size).toBe(2);
  });
});

describe("outbox projection", () => {
  it("treats an already-deleted reaction intention as complete", async () => {
    const operation = likeOperation();
    const db = database();
    db.upsert.mockImplementation((table) => {
      if (table === app.pendingOperations) {
        throw new Error(`Upsert failed: WriteError("row already deleted: ${operation.id}")`);
      }
      return settledWrite();
    });

    await expect(
      createProjection(db).projectReactionOperation(
        operation,
        {
          ...post(operation.payload.subjectUri, operation.createdAt),
          viewer: { like: `at://${operation.ownerDid}/app.bsky.feed.like/${operation.rkey}` },
        },
        { uri: `at://${operation.ownerDid}/app.bsky.feed.like/${operation.rkey}` },
      ),
    ).resolves.toBeUndefined();
  });
});

describe("thread projection", () => {
  it("projects stable parent relationships without duplicating repeated replies", async () => {
    const root = post("at://did:plc:author/app.bsky.feed.post/root");
    const reply = post("at://did:plc:author/app.bsky.feed.post/reply");
    reply.record!.reply = {
      root: { uri: root.uri, cid: root.cid },
      parent: { uri: root.uri, cid: root.cid },
    };
    const repeated = { $type: "app.bsky.feed.defs#threadViewPost", post: reply } as const;
    const thread = {
      $type: "app.bsky.feed.defs#threadViewPost",
      post: root,
      replies: [repeated, repeated],
    } as const;
    const db = database();

    const result = await createProjection(db).projectThread("did:plc:viewer", root.uri!, thread);

    expect(result.count).toBe(2);
    expect(db.upsert.mock.calls.filter(([table]) => table === app.threadEntries)).toHaveLength(2);
  });
});
