import { describe, expect, it } from "vitest";
import { normalizePost, normalizeTimelineItem, stableObjectId } from "./timeline.js";

describe("timeline ingestion", () => {
  it("normalises a quoted post alongside its outer post", () => {
    const quotedUri = "at://did:plc:quoted/app.bsky.feed.post/3m12345678920";
    const normalized = normalizePost({
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678921",
      cid: "bafyouter",
      author: { did: "did:plc:author", handle: "author.test" },
      record: { text: "Worth reading", createdAt: "2026-07-17T08:00:00.000Z" },
      indexedAt: "2026-07-17T08:00:01.000Z",
      embed: {
        record: {
          $type: "app.bsky.embed.record#view",
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
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678922",
      cid: "bafyouter",
      author: { did: "did:plc:author" },
      record: { text: "Quote with my own image", createdAt: "2026-07-17T08:00:00.000Z" },
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

  it("anchors a reply thread to the root post time", () => {
    const root = {
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678920",
      cid: "bafyroot",
      author: { did: "did:plc:author" },
      record: { text: "Root", createdAt: "2026-07-15T07:00:00.000Z" },
      indexedAt: "2026-07-15T07:00:01.000Z",
    };
    const reply = {
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678921",
      cid: "bafyreply",
      author: { did: "did:plc:author" },
      record: {
        text: "Much later reply",
        createdAt: "2026-07-16T10:00:00.000Z",
        reply: { root: { uri: root.uri, cid: root.cid }, parent: { uri: root.uri, cid: root.cid } },
      },
      indexedAt: "2026-07-16T10:00:01.000Z",
    };

    expect(normalizeTimelineItem("did:plc:viewer", { post: reply, reply: { root, parent: root } })!
      .timelineEntry.sortAt).toBe(root.indexedAt);
  });

  it("keeps a direct post and a repost as separate timeline events over one post row", () => {
    const post = {
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678921",
      cid: "bafypost",
      author: { did: "did:plc:author", handle: "author.test" },
      record: { text: "One shared post", createdAt: "2026-07-15T07:00:00.000Z" },
      indexedAt: "2026-07-15T07:00:01.000Z",
    };
    const direct = normalizeTimelineItem("did:plc:viewer", { post });
    const repost = normalizeTimelineItem("did:plc:viewer", {
      post,
      reason: {
        $type: "app.bsky.feed.defs#reasonRepost",
        by: { did: "did:plc:reposter", handle: "reposter.test" },
        uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
        indexedAt: "2026-07-15T09:00:00.000Z",
      },
    });

    expect(direct!.post.id).toBe(repost!.post.id);
    expect(direct!.timelineEntry.id).not.toBe(repost!.timelineEntry.id);
    expect(direct!.timelineEntry.repostId).toBeUndefined();
    expect(repost!.timelineEntry.repostId).toBeDefined();
  });

  it("preserves a repost as a distinct feed event while deduplicating its post", () => {
    const normalized = normalizeTimelineItem("did:plc:viewer", {
      post: {
        uri: "at://did:plc:author/app.bsky.feed.post/3m12345678922",
        cid: "bafypost",
        author: {
          did: "did:plc:author",
          handle: "author.test",
          displayName: "Original Author",
          avatar: "https://cdn.example/author.jpg",
        },
        record: {
          text: "A post worth sharing",
          createdAt: "2026-07-15T08:00:00.000Z",
        },
        indexedAt: "2026-07-15T08:00:01.000Z",
        replyCount: 2,
        likeCount: 4,
        repostCount: 3,
        viewer: {
          repost: "at://did:plc:viewer/app.bsky.feed.repost/3m12345678923",
        },
      },
      reason: {
        $type: "app.bsky.feed.defs#reasonRepost",
        by: {
          did: "did:plc:reposter",
          handle: "reposter.test",
          displayName: "Reposter",
          avatar: "https://cdn.example/reposter.jpg",
        },
        uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
        cid: "bafyrepost",
        indexedAt: "2026-07-15T09:00:00.000Z",
      },
    });

    expect(normalized).not.toBeNull();
    expect(normalized!.profiles.map((profile) => profile.did)).toEqual([
      "did:plc:author",
      "did:plc:reposter",
    ]);
    expect(normalized!.post).toMatchObject({
      uri: "at://did:plc:author/app.bsky.feed.post/3m12345678922",
      replyCount: 2,
      likeCount: 4,
      repostCount: 3,
    });
    expect(normalized!.reposts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          actorDid: "did:plc:reposter",
          uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
          subjectPostId: normalized!.post.id,
          active: true,
        }),
        expect.objectContaining({
          actorDid: "did:plc:viewer",
          uri: "at://did:plc:viewer/app.bsky.feed.repost/3m12345678923",
          subjectPostId: normalized!.post.id,
          active: true,
        }),
      ]),
    );
    expect(normalized!.timelineEntry).toMatchObject({
      ownerDid: "did:plc:viewer",
      postId: normalized!.post.id,
      repostId: normalized!.reposts.find((repost) => repost.actorDid === "did:plc:reposter")!.id,
      sortAt: "2026-07-15T09:00:00.000Z",
      active: true,
    });
    expect(normalized!.timelineEntry.id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });
});
