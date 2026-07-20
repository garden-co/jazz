import { describe, expect, it } from "vitest";
import { normalizeTimelineItem } from "./projection-model.js";

describe("projection model", () => {
  it("keeps a post bundle intact and projects only the repost feed event", () => {
    const normalized = normalizeTimelineItem("did:plc:viewer", {
      post: {
        uri: "at://did:plc:author/app.bsky.feed.post/3m12345678922",
        cid: "bafypost",
        author: { did: "did:plc:author", handle: "author.test" },
        record: { text: "A post worth sharing", createdAt: "2026-07-15T08:00:00.000Z" },
        indexedAt: "2026-07-15T08:00:01.000Z",
        viewer: {
          repost: "at://did:plc:viewer/app.bsky.feed.repost/3m12345678923",
        },
      },
      reason: {
        $type: "app.bsky.feed.defs#reasonRepost",
        by: { did: "did:plc:reposter", handle: "reposter.test" },
        uri: "at://did:plc:reposter/app.bsky.feed.repost/3m12345678924",
        indexedAt: "2026-07-15T09:00:00.000Z",
      },
    });

    expect(normalized?.postBundle).toMatchObject({
      profile: { handle: "author.test" },
      post: { text: "A post worth sharing" },
    });
    expect(normalized?.reposterProfile).toMatchObject({ handle: "reposter.test" });
    expect(normalized?.repost).toMatchObject({
      actorDid: "did:plc:reposter",
      subjectPostId: normalized?.postBundle.post.id,
    });
    expect(normalized).not.toHaveProperty("likes");
    expect(normalized).not.toHaveProperty("reposts");
  });
});
