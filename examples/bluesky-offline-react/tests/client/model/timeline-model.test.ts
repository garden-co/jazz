import { describe, expect, it } from "vitest";
import {
  buildTimeline,
  hydrateTimelineThread,
  writableReplyCount,
  type IncludedPost,
  type TimelineEntryView,
} from "../../../src/model/timeline-data.js";

function post(id: string, replyParentId?: string): IncludedPost {
  return {
    id,
    uri: `at://did:plc:author/app.bsky.feed.post/${id}`,
    cid: `cid-${id}`,
    authorDid: "did:plc:author",
    authorProfileId: "profile",
    text: id,
    facetsJson: null,
    createdAt: "2026-07-16T10:00:00.000Z",
    indexedAt: "2026-07-16T10:00:00.000Z",
    replyParentId: replyParentId ?? null,
    replyRootId: null,
    quotedPostId: null,
    replyCount: 0,
    likeCount: 0,
    repostCount: 0,
    state: "synced",
  };
}

describe("timeline model", () => {
  it("hydrates a visible thread separately from its timeline row", () => {
    const root = post("root");
    const reply = post("reply", root.id);
    const [item] = buildTimeline([{
      id: "entry",
      ownerDid: "did:plc:viewer",
      postId: root.id,
      threadRootId: root.id,
      repostId: null,
      sortAt: root.indexedAt,
      active: true,
      post: root,
      threadRoot: root,
    }]);

    expect(hydrateTimelineThread(item!, [{
      id: "thread-reply",
      rootPostId: root.id,
      postId: reply.id,
      parentPostId: root.id,
      state: "post",
      sortOrder: 1,
      indexedAt: reply.indexedAt,
      post: reply,
    }]).node.replies.map(({ post }) => post.id)).toEqual([reply.id]);
  });

  it("only changes a reply count optimistically when the viewer owns the parent post", () => {
    const parent = post("parent");

    expect(writableReplyCount(parent, "did:plc:viewer")).toBeUndefined();
    expect(writableReplyCount(parent, parent.authorDid)).toBe(1);
  });

  it("includes the quoted post in the display model", () => {
    const quote = { ...post("quote"), authorProfile: { id: "quote-profile", did: "did:plc:quoted", handle: "quoted.test", displayName: null, description: null, avatar: null, indexedAt: "2026-07-16T10:00:00.000Z" } };
    const outer = { ...post("outer"), quotedPostId: quote.id, quotedPost: quote };
    const row: TimelineEntryView = {
      id: "entry",
      ownerDid: "viewer",
      postId: outer.id,
      threadRootId: outer.id,
      repostId: null,
      sortAt: outer.indexedAt,
      active: true,
      post: outer,
      threadRoot: outer,
    };

    expect(buildTimeline([row])[0]?.node.post.quote).toMatchObject({
      id: quote.id,
      authorProfile: { handle: "quoted.test" },
      images: [],
    });
  });

  it("orders equal-time rows and replies deterministically", () => {
    const root = post("root");
    const replyA = post("reply-a", root.id);
    const replyB = post("reply-b", root.id);
    const threadRoot = {
      ...root,
      threadEntriesViaRootPost: [replyB, root, replyA].map((threadPost) => ({
        id: `thread-${threadPost.id}`,
        rootPostId: root.id,
        postId: threadPost.id,
        parentPostId: threadPost.replyParentId,
        state: "post" as const,
        sortOrder: 0,
        indexedAt: threadPost.indexedAt,
        post: threadPost,
      })),
    };
    const rows: TimelineEntryView[] = [
      { id: "entry-b", ownerDid: "viewer", postId: replyB.id, threadRootId: root.id, repostId: null, sortAt: replyB.indexedAt, active: true, post: replyB, threadRoot },
      { id: "entry-a", ownerDid: "viewer", postId: replyA.id, threadRootId: root.id, repostId: null, sortAt: replyA.indexedAt, active: true, post: replyA, threadRoot },
    ];

    const forwards = buildTimeline(rows);
    const backwards = buildTimeline([...rows].reverse());

    expect(forwards).toEqual(backwards);
    expect(forwards[0]?.id).toBe(`thread:${root.id}`);
    expect(forwards[0]?.node.replies.map((reply) => reply.post.id)).toEqual([replyA.id, replyB.id]);
  });

  it("shows a reposted reply without its ancestors and links to its thread", () => {
    const root = post("root");
    const parent = post("parent", root.id);
    const reply = post("reply", parent.id);
    const row: TimelineEntryView = {
      id: "entry",
      ownerDid: "did:plc:viewer",
      postId: reply.id,
      threadRootId: root.id,
      repostId: "repost",
      sortAt: reply.indexedAt,
      active: true,
      post: reply,
      repost: {
        id: "repost",
        uri: null,
        cid: null,
        active: true,
        actorDid: "did:plc:reposter",
        actorProfileId: "reposter-profile",
        subjectPostId: reply.id,
        createdAt: reply.createdAt,
      },
      threadRoot: {
        ...root,
        threadEntriesViaRootPost: [root, parent, reply].map((threadPost) => ({
          id: `entry-${threadPost.id}`,
          rootPostId: root.id,
          postId: threadPost.id,
          parentPostId: threadPost.replyParentId,
          state: "post" as const,
          sortOrder: 0,
          indexedAt: threadPost.indexedAt,
          post: threadPost,
        })),
      },
    };

    expect(buildTimeline([row])[0]).toMatchObject({
      node: { post: { id: reply.id }, replies: [] },
      threadRoot: { id: root.id },
      threadUrl: "https://bsky.app/profile/did:plc:author/post/reply",
    });
  });

  it("shows the same post only once when it has multiple top-level events", () => {
    const sharedPost = post("shared");
    const duplicatePost = { ...post("shared-copy"), uri: sharedPost.uri };
    const direct: TimelineEntryView = {
      id: "direct-entry",
      ownerDid: "did:plc:viewer",
      postId: sharedPost.id,
      threadRootId: sharedPost.id,
      repostId: null,
      sortAt: "2026-07-16T10:00:00.000Z",
      active: true,
      post: sharedPost,
      threadRoot: sharedPost,
    };
    const repost: TimelineEntryView = {
      ...direct,
      id: "repost-entry",
      postId: duplicatePost.id,
      threadRootId: duplicatePost.id,
      repostId: "repost",
      sortAt: "2026-07-16T11:00:00.000Z",
      post: duplicatePost,
      threadRoot: duplicatePost,
      repost: {
        id: "repost",
        uri: null,
        cid: null,
        active: true,
        actorDid: "did:plc:reposter",
        actorProfileId: "reposter-profile",
        subjectPostId: duplicatePost.id,
        createdAt: "2026-07-16T11:00:00.000Z",
      },
    };

    expect(buildTimeline([direct, repost])).toMatchObject([
      { id: "repost-entry", node: { post: { uri: sharedPost.uri } } },
    ]);
  });
});
