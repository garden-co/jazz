import { describe, expect, it } from "vitest";
import {
  buildTimeline,
  hydrateTimelineThread,
  writableReplyCount,
  type IncludedPost,
  type TimelineEntryView,
  type TimelineRelations,
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

function profile(id = "profile", handle = "author.test") {
  return {
    id,
    did: `did:plc:${id}`,
    handle,
    displayName: null,
    description: null,
    avatar: null,
    indexedAt: "2026-07-16T10:00:00.000Z",
  };
}

function relations(
  posts: IncludedPost[],
  extra: Partial<TimelineRelations> = {},
): TimelineRelations {
  return {
    posts,
    profiles: [profile(), ...(extra.profiles ?? [])],
    images: extra.images ?? [],
    likes: extra.likes ?? [],
    reposts: extra.reposts ?? [],
  };
}

describe("timeline model", () => {
  it("hydrates a visible thread separately from its timeline row", () => {
    const root = post("root");
    const reply = post("reply", root.id);
    const [item] = buildTimeline(
      [
        {
          id: "entry",
          ownerDid: "did:plc:viewer",
          postId: root.id,
          threadRootId: root.id,
          repostId: null,
          sortAt: root.indexedAt,
          active: true,
        },
      ],
      relations([root]),
    );

    expect(
      hydrateTimelineThread(item!, [reply], relations([root, reply])).node.replies.map(
        ({ post }) => post.id,
      ),
    ).toEqual([reply.id]);
  });

  it("only changes a reply count optimistically when the viewer owns the parent post", () => {
    const parent = post("parent");

    expect(writableReplyCount(parent, "did:plc:viewer")).toBeUndefined();
    expect(writableReplyCount(parent, parent.authorDid)).toBe(1);
  });

  it("includes the quoted post in the display model", () => {
    const quote = { ...post("quote"), authorProfileId: "quote-profile" };
    const outer = { ...post("outer"), quotedPostId: quote.id };
    const row: TimelineEntryView = {
      id: "entry",
      ownerDid: "viewer",
      postId: outer.id,
      threadRootId: outer.id,
      repostId: null,
      sortAt: outer.indexedAt,
      active: true,
    };

    expect(
      buildTimeline(
        [row],
        relations([outer, quote], {
          profiles: [profile("quote-profile", "quoted.test")],
        }),
      )[0]?.node.post.quote,
    ).toMatchObject({
      id: quote.id,
      authorProfile: { handle: "quoted.test" },
      images: [],
    });
  });

  it("orders equal-time rows and replies deterministically", () => {
    const root = post("root");
    const replyA = post("reply-a", root.id);
    const replyB = post("reply-b", root.id);
    const rows: TimelineEntryView[] = [
      {
        id: "entry-b",
        ownerDid: "viewer",
        postId: replyB.id,
        threadRootId: root.id,
        repostId: null,
        sortAt: replyB.indexedAt,
        active: true,
      },
      {
        id: "entry-a",
        ownerDid: "viewer",
        postId: replyA.id,
        threadRootId: root.id,
        repostId: null,
        sortAt: replyA.indexedAt,
        active: true,
      },
    ];
    const data = relations([root, replyA, replyB]);

    const forwards = [hydrateTimelineThread(buildTimeline(rows, data)[0]!, [replyB, replyA], data)];
    const backwards = [
      hydrateTimelineThread(buildTimeline([...rows].reverse(), data)[0]!, [replyA, replyB], data),
    ];

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
    };
    const repost = {
      id: "repost",
      uri: null,
      cid: null,
      active: true,
      actorDid: "did:plc:reposter",
      actorProfileId: "reposter-profile",
      subjectPostId: reply.id,
      createdAt: reply.createdAt,
    };

    expect(
      buildTimeline(
        [row],
        relations([root, parent, reply], {
          reposts: [repost],
        }),
      )[0],
    ).toMatchObject({
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
    };
    const repost: TimelineEntryView = {
      ...direct,
      id: "repost-entry",
      postId: duplicatePost.id,
      threadRootId: duplicatePost.id,
      repostId: "repost",
      sortAt: "2026-07-16T11:00:00.000Z",
    };
    const repostRow = {
      id: "repost",
      uri: null,
      cid: null,
      active: true,
      actorDid: "did:plc:reposter",
      actorProfileId: "reposter-profile",
      subjectPostId: duplicatePost.id,
      createdAt: "2026-07-16T11:00:00.000Z",
    };

    expect(
      buildTimeline(
        [direct, repost],
        relations([sharedPost, duplicatePost], {
          reposts: [repostRow],
        }),
      ),
    ).toMatchObject([{ id: "repost-entry", node: { post: { uri: sharedPost.uri } } }]);
  });
});
