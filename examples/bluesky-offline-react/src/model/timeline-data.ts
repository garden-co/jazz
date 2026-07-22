import { schema as s } from "jazz-tools";
import { app } from "../../schema.js";

export function timelineQuery(ownerDid: string) {
  return app.timelineEntries
    .where({ ownerDid: { eq: ownerDid }, active: { eq: true } })
    .orderBy("sortAt", "desc");
}

type PostRow = s.RowOf<typeof app.posts>;
type ProfileView = s.RowOf<typeof app.profiles>;
type RepostRow = s.RowOf<typeof app.reposts>;

export type ThreadPostView = PostRow;
export type TimelineEntryView = s.RowOf<ReturnType<typeof timelineQuery>>;
export type IncludedPost = PostRow;
export type IncludedRepost = RepostRow & { actorProfile?: ProfileView };
export type ImageView = s.RowOf<typeof app.postImages>;
export type ReactionView = s.RowOf<typeof app.likes> | s.RowOf<typeof app.reposts>;

export type TimelineRelations = {
  viewerDid?: string;
  posts: PostRow[];
  profiles: ProfileView[];
  images: ImageView[];
  likes: s.RowOf<typeof app.likes>[];
  reposts: s.RowOf<typeof app.reposts>[];
};

const emptyRelations: TimelineRelations = {
  posts: [],
  profiles: [],
  images: [],
  likes: [],
  reposts: [],
};

export type DisplayPost = PostRow & {
  authorProfile?: ProfileView;
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
  ownerDid: string;
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

function toDisplayPost(
  post: PostRow,
  relations: TimelineRelations,
  seen = new Set<string>(),
): DisplayPost {
  const quote =
    post.quotedPostId && !seen.has(post.quotedPostId)
      ? relations.posts.find((candidate) => candidate.id === post.quotedPostId)
      : undefined;
  return {
    ...post,
    authorProfile: relations.profiles.find((profile) => profile.id === post.authorProfileId),
    images: relations.images
      .filter((image) => image.postId === post.id)
      .sort((a, b) => a.position - b.position),
    quote: quote ? toDisplayPost(quote, relations, new Set(seen).add(post.id)) : undefined,
    like: relations.likes.find((like) => like.subjectPostId === post.id),
    repost: relations.reposts.find(
      (repost) => repost.subjectPostId === post.id && repost.actorDid === relations.viewerDid,
    ),
  };
}

export function buildThread(rootId: string, posts: IncludedPost[], relations = emptyRelations) {
  return buildDisplayThread(
    rootId,
    posts.map((post) => toDisplayPost(post, relations)),
  );
}

function buildDisplayThread(rootId: string, posts: DisplayPost[]) {
  const unique = new Map(posts.map((post) => [post.id, post]));
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
    node.replies.sort(
      (a, b) =>
        Number(b.post.authorDid === authorDid) - Number(a.post.authorDid === authorDid) ||
        a.post.createdAt.localeCompare(b.post.createdAt) ||
        a.post.id.localeCompare(b.post.id),
    );
    for (const reply of node.replies) sortReplies(reply, authorDid);
  };
  sortReplies(root, root.post.authorDid);
  return root;
}

export function buildTimeline(rows: TimelineEntryView[], relations = emptyRelations) {
  const items: TimelineItem[] = [];
  const seenPostUris = new Set<string>();
  const seenThreads = new Set<string>();
  const orderedRows = [...rows].sort(
    (a, b) => b.sortAt.localeCompare(a.sortAt) || a.id.localeCompare(b.id),
  );
  for (const row of orderedRows) {
    const post = relations.posts.find((candidate) => candidate.id === row.postId);
    const threadRoot = relations.posts.find((candidate) => candidate.id === row.threadRootId);
    if (!post || !threadRoot || seenPostUris.has(post.uri)) continue;
    seenPostUris.add(post.uri);
    if (!row.repostId && seenThreads.has(row.threadRootId)) continue;
    if (!row.repostId) seenThreads.add(row.threadRootId);
    const repostedReply = Boolean(row.repostId && post.replyParentId);
    const node = repostedReply
      ? buildThread(post.id, [post], relations)
      : (buildThread(row.threadRootId, [threadRoot, post], relations) ??
        buildThread(post.id, [post], relations));
    const repost = row.repostId
      ? relations.reposts.find((candidate) => candidate.id === row.repostId)
      : undefined;
    if (node)
      items.push({
        id: row.repostId ? row.id : `thread:${row.threadRootId}`,
        ownerDid: row.ownerDid,
        node,
        threadRoot: toDisplayPost(threadRoot, relations),
        repost: repost && {
          ...repost,
          actorProfile: relations.profiles.find((profile) => profile.id === repost.actorProfileId),
        },
        threadUrl: repostedReply
          ? `https://bsky.app/profile/${post.authorDid}/post/${post.uri.split("/").at(-1)}`
          : undefined,
      });
  }
  return items;
}

export function hydrateTimelineThread(
  item: TimelineItem,
  threadPosts: ThreadPostView[],
  relations = emptyRelations,
) {
  if (item.threadUrl) return item;
  const posts = threadPosts.map((post) => toDisplayPost(post, relations));
  posts.push(item.threadRoot, item.node.post);
  return {
    ...item,
    node: buildDisplayThread(item.threadRoot.id, posts) ?? item.node,
  };
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
