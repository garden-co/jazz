import { describe, expect, it } from "vitest";
import { normalizeTimelineItem } from "./timeline.js";

describe("timeline ingestion", () => {
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
