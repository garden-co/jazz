import { schema as s } from "jazz-tools";
import { app } from "../schema.js";

function includedPost(ownerDid: string) {
  const quotedPost = app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
  }).requireIncludes();
  return app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
    quotedPost,
    likesViaSubjectPost: app.likes.where({ actorDid: { eq: ownerDid } }),
    repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: ownerDid } }),
  }).requireIncludes();
}

function timelineQueryWithThreadDetails(ownerDid: string) {
  const post = includedPost(ownerDid);
  const threadRoot = app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
    quotedPost: app.posts.include({
      authorProfile: true,
      postImagesViaPost: true,
    }).requireIncludes(),
    likesViaSubjectPost: app.likes.where({ actorDid: { eq: ownerDid } }),
    repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: ownerDid } }),
    threadEntriesViaRootPost: app.threadEntries
      .orderBy("sortOrder", "asc")
      .include({ post })
      .requireIncludes(),
  }).requireIncludes();
  return app.timelineEntries
    .where({ ownerDid: { eq: ownerDid }, active: { eq: true } })
    .orderBy("sortAt", "desc")
    .include({
      post,
      repost: app.reposts.include({ actorProfile: true }).requireIncludes(),
      threadRoot,
    })
    .requireIncludes();
}

function timelineSummaryQuery(ownerDid: string) {
  const post = includedPost(ownerDid);
  return app.timelineEntries
    .where({ ownerDid: { eq: ownerDid }, active: { eq: true } })
    .orderBy("sortAt", "desc")
    .include({
      post,
      repost: app.reposts.include({ actorProfile: true }).requireIncludes(),
      threadRoot: post,
    })
    .requireIncludes();
}

export function timelineQuery(ownerDid: string, includeThreadDetails = true) {
  return includeThreadDetails
    ? timelineQueryWithThreadDetails(ownerDid)
    : timelineSummaryQuery(ownerDid);
}

type DetailedTimelineEntryView = s.RowOf<ReturnType<typeof timelineQueryWithThreadDetails>>;
type SummaryTimelineEntryView = s.RowOf<ReturnType<typeof timelineSummaryQuery>>;
export type TimelineEntryView = DetailedTimelineEntryView | SummaryTimelineEntryView;
export type IncludedPost = DetailedTimelineEntryView["post"];
export type IncludedRepost = NonNullable<TimelineEntryView["repost"]>;
export type ImageView = IncludedPost["postImagesViaPost"][number];
export type ReactionView =
  | IncludedPost["likesViaSubjectPost"][number]
  | IncludedPost["repostsViaSubjectPost"][number];

type PostRow = s.RowOf<typeof app.posts>;
type ProfileView = IncludedPost["authorProfile"];
type DisplaySourcePost =
  | IncludedPost
  | NonNullable<IncludedPost["quotedPost"]>
  | TimelineEntryView["threadRoot"];

export type DisplayPost = PostRow & {
  authorProfile: ProfileView;
  images: ImageView[];
  quote?: DisplayPost;
  like?: ReactionView;
  repost?: ReactionView;
};

export type TimelinePostNode = {
  post: DisplayPost;
  replies: TimelinePostNode[];
};

export type TimelineItem = {
  id: string;
  node: TimelinePostNode;
  threadRoot: DisplayPost;
  repost?: IncludedRepost | null;
  threadUrl?: string;
};

// Clients may only update projected post rows that they own; other reply counts
// are reconciled later when the BFF receives the updated AppView post.
export function writableReplyCount(
  parent: Pick<IncludedPost, "authorDid" | "replyCount">,
  viewerDid: string,
) {
  return parent.authorDid === viewerDid ? parent.replyCount + 1 : undefined;
}

function toDisplayPost(post: DisplaySourcePost): DisplayPost {
  const images = "postImagesViaPost" in post ? post.postImagesViaPost : [];
  const quote = "quotedPost" in post ? post.quotedPost : null;
  const likes = "likesViaSubjectPost" in post ? post.likesViaSubjectPost : [];
  const reposts = "repostsViaSubjectPost" in post ? post.repostsViaSubjectPost : [];
  return {
    ...post,
    images: [...images].sort((a, b) => a.position - b.position),
    quote: quote ? toDisplayPost(quote) : undefined,
    like: likes[0],
    repost: reposts[0],
  };
}

export function buildThread(rootId: string, posts: IncludedPost[]) {
  const unique = new Map(posts.map((post) => [post.id, toDisplayPost(post)]));
  const nodes = new Map<string, TimelinePostNode>(
    [...unique].map(([id, post]) => [id, { post, replies: [] }]),
  );
  const roots: TimelinePostNode[] = [];
  for (const node of nodes.values()) {
    const parent = node.post.replyParentId ? nodes.get(node.post.replyParentId) : undefined;
    if (parent) parent.replies.push(node);
    else roots.push(node);
  }
  const root = nodes.get(rootId) ?? roots[0];
  if (!root) return undefined;
  const sortReplies = (node: TimelinePostNode, authorDid: string) => {
    node.replies.sort((a, b) =>
      Number(b.post.authorDid === authorDid) - Number(a.post.authorDid === authorDid)
      || a.post.createdAt.localeCompare(b.post.createdAt)
      || a.post.id.localeCompare(b.post.id));
    for (const reply of node.replies) sortReplies(reply, authorDid);
  };
  sortReplies(root, root.post.authorDid);
  return root;
}

export function buildTimeline(rows: TimelineEntryView[]) {
  const items: TimelineItem[] = [];
  const seenThreads = new Set<string>();
  const orderedRows = [...rows].sort((a, b) =>
    b.sortAt.localeCompare(a.sortAt) || a.id.localeCompare(b.id));
  for (const row of orderedRows) {
    if (!row.repostId && seenThreads.has(row.threadRootId)) continue;
    if (!row.repostId) seenThreads.add(row.threadRootId);
    const repostedReply = Boolean(row.repostId && row.post.replyParentId);
    const threadEntries = hasThreadDetails(row)
      ? row.threadRoot.threadEntriesViaRootPost
      : [];
    const threadPosts = threadEntries
      .flatMap((entry) => entry.state === "post" ? [entry.post] : []);
    if (!threadPosts.some((post) => post.id === row.post.id)) threadPosts.push(row.post);
    if (!threadPosts.some((post) => post.id === row.threadRoot.id)) threadPosts.push(row.threadRoot);
    const node = repostedReply
      ? buildThread(row.post.id, [row.post])
      : buildThread(row.threadRootId, threadPosts) ?? buildThread(row.post.id, [row.post]);
    if (node) items.push({
      id: row.repostId ? row.id : `thread:${row.threadRootId}`,
      node,
      threadRoot: toDisplayPost(row.threadRoot),
      repost: row.repost,
      threadUrl: repostedReply
        ? `https://bsky.app/profile/${row.post.authorDid}/post/${row.post.uri.split("/").at(-1)}`
        : undefined,
    });
  }
  return items;
}

function hasThreadDetails(row: TimelineEntryView): row is DetailedTimelineEntryView {
  return "threadEntriesViaRootPost" in row.threadRoot;
}

export const timelinePageSize = 20;
export const initialTimelineLimit = timelinePageSize;

export function timelineQueryLimit(visibleLimit: number) {
  return visibleLimit + 1;
}

export function nextTimelineLimit(currentLimit: number) {
  return currentLimit + timelinePageSize;
}

export function windowTimelineRows<Row>(rows: Row[], visibleLimit: number) {
  return {
    rows: rows.slice(0, visibleLimit),
    hasMore: rows.length > visibleLimit,
  };
}
