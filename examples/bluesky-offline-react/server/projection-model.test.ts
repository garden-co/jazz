import { describe, expect, it } from "vitest";
import {
  flattenThread,
  normalizePost,
  normalizeTimelineItem,
  stableObjectId,
  type PostView,
} from "./projection-model.js";

function post(uri: string, indexedAt = "2026-07-15T08:00:01.000Z"): PostView {
  return {
    uri,
    cid: `bafy-${uri.split("/").at(-1)}`,
    author: { did: "did:plc:author", handle: "author.test" },
    record: { text: "A post", createdAt: indexedAt },
    indexedAt,
  };
}

describe("post projection", () => {
  it("normalises a quoted post alongside its outer post", () => {
    const quotedUri = "at://did:plc:quoted/app.bsky.feed.post/3m12345678920";
    const normalized = normalizePost({
      ...post("at://did:plc:author/app.bsky.feed.post/3m12345678921"),
      record: { text: "Worth reading", createdAt: "2026-07-17T08:00:00.000Z" },
      embed: {
        record: {
          uri: quotedUri,
          cid: "bafyquoted",
          author: { did: "did:plc:quoted", handle: "quoted.test" },
          value: { text: "The quoted post", createdAt: "2026-07-17T07:00:00.000Z" },
          indexedAt: "2026-07-17T07:00:01.000Z",
        },
      },
    });

    expect(normalized?.post.quotedPostId).toBe(stableObjectId("bluesky-post", quotedUri));
    expect(normalized?.quote).toMatchObject({
      profile: { handle: "quoted.test" },
      post: { uri: quotedUri, text: "The quoted post" },
    });
  });

  it("normalises record-with-media quotes", () => {
    const normalized = normalizePost({
      ...post("at://did:plc:author/app.bsky.feed.post/3m12345678922"),
      embed: {
        media: { images: [{ thumb: "https://cdn.test/outer-thumb", fullsize: "https://cdn.test/outer" }] },
        record: {
          record: {
            uri: "at://did:plc:quoted/app.bsky.feed.post/3m12345678920",
            cid: "bafyquoted",
            author: { did: "did:plc:quoted" },
            value: { text: "Quoted", createdAt: "2026-07-17T07:00:00.000Z" },
          },
        },
      },
    });

    expect(normalized?.post.quotedPostId).toBe(normalized?.quote?.post.id);
    expect(normalized?.images).toHaveLength(1);
  });
});

describe("timeline projection", () => {
  it("anchors a reply thread to the root post time", () => {
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

    expect(normalizeTimelineItem("did:plc:viewer", {
      post: reply,
      reply: { root, parent: root },
    })?.timelineEntry.sortAt).toBe(root.indexedAt);
  });

  it("keeps direct and repost events distinct without duplicating viewer reactions", () => {
    const sharedPost = {
      ...post("at://did:plc:author/app.bsky.feed.post/3m12345678922"),
      viewer: {
        like: "at://did:plc:viewer/app.bsky.feed.like/3m12345678923",
        repost: "at://did:plc:viewer/app.bsky.feed.repost/3m12345678923",
      },
    };
    const direct = normalizeTimelineItem("did:plc:viewer", { post: sharedPost });
    const repost = normalizeTimelineItem("did:plc:viewer", {
      post: sharedPost,
      reason: {
        $type: "app.bsky.feed.defs#reasonRepost",
        by: { did: "did:plc:reposter", handle: "reposter.test" },
        uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
        indexedAt: "2026-07-15T09:00:00.000Z",
      },
    });

    expect(direct?.postBundle.post.id).toBe(repost?.postBundle.post.id);
    expect(direct?.timelineEntry.id).not.toBe(repost?.timelineEntry.id);
    expect(direct?.timelineEntry.repostId).toBeUndefined();
    expect(repost).toMatchObject({
      postBundle: { profile: { handle: "author.test" } },
      reposterProfile: { handle: "reposter.test" },
      repost: {
        actorDid: "did:plc:reposter",
        subjectPostId: repost?.postBundle.post.id,
        active: true,
      },
      timelineEntry: {
        repostId: repost?.repost?.id,
        sortAt: "2026-07-15T09:00:00.000Z",
      },
    });
    expect(repost).not.toHaveProperty("likes");
    expect(repost).not.toHaveProperty("reposts");
  });
});

describe("thread projection", () => {
  it("flattens posts and unavailable replies with stable parent relationships", () => {
    const root = { post: { uri: "at://author/app.bsky.feed.post/root" } };
    const selected = {
      post: {
        uri: "at://author/app.bsky.feed.post/selected",
        record: { reply: { root: { uri: root.post.uri }, parent: { uri: root.post.uri } } },
      },
      parent: root,
      replies: [{
        post: { uri: "at://reply/app.bsky.feed.post/child" },
        replies: [
          { uri: "at://missing/app.bsky.feed.post/missing", notFound: true },
          { uri: "at://blocked/app.bsky.feed.post/blocked", blocked: true },
        ],
      }],
    };

    const result = flattenThread(selected.post.uri, selected);

    expect(result.rootPostId).toBe(root.post.uri);
    expect(result).not.toHaveProperty("selectedPostId");
    expect(result.entries).toEqual([
      expect.objectContaining({ post: root.post, postId: root.post.uri, state: "post", sortOrder: 0 }),
      expect.objectContaining({ post: selected.post, postId: selected.post.uri, parentPostId: root.post.uri, state: "post", sortOrder: 1 }),
      expect.objectContaining({ postId: "at://reply/app.bsky.feed.post/child", parentPostId: selected.post.uri, state: "post", sortOrder: 2 }),
      expect.objectContaining({ post: undefined, postId: "at://missing/app.bsky.feed.post/missing", state: "not-found", sortOrder: 3 }),
      expect.objectContaining({ post: undefined, postId: "at://blocked/app.bsky.feed.post/blocked", state: "blocked", sortOrder: 4 }),
    ]);
    expect(result.entries.every((entry) => !("node" in entry))).toBe(true);
  });

  it("does not duplicate a reply repeated by an AppView response", () => {
    const reply = { post: { uri: "at://author/app.bsky.feed.post/reply" } };
    const thread = {
      post: { uri: "at://author/app.bsky.feed.post/root" },
      replies: [reply, reply],
    };

    expect(flattenThread(thread.post.uri, thread).entries).toHaveLength(2);
  });
});
