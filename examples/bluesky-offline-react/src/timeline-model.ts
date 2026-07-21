import { schema as s } from "jazz-tools";
import { app } from "../shared/schema.js";

type ProfileRow = s.RowOf<typeof app.profiles>;
type PostRow = s.RowOf<typeof app.posts>;
type ImageRow = s.RowOf<typeof app.postImages>;
type LikeRow = s.RowOf<typeof app.likes>;
type RepostRow = s.RowOf<typeof app.reposts>;
type ThreadEntryRow = s.RowOf<typeof app.threadEntries>;
type TimelineEntryRow = s.RowOf<typeof app.timelineEntries>;

export type IncludedPost = PostRow & {
  authorProfile?: ProfileRow | null;
  postImagesViaPost?: ImageRow[];
  likesViaSubjectPost?: LikeRow[];
  repostsViaSubjectPost?: RepostRow[];
  quotedPost?: IncludedPost | null;
  threadEntriesViaRootPost?: Array<ThreadEntryRow & { post?: IncludedPost | null }>;
};
export type IncludedRepost = RepostRow & { actorProfile?: ProfileRow | null };
export type ImageView = ImageRow;
export type ReactionView = LikeRow | RepostRow;
export type TimelineEntryView = TimelineEntryRow & {
  post?: IncludedPost | null;
  threadRoot?: IncludedPost | null;
  repost?: IncludedRepost | null;
};

export type DisplayPost = IncludedPost & {
  images: ImageView[];
  quote?: DisplayPost;
  like?: ReactionView;
  repost?: ReactionView;
};

export type TimelinePostNode = {
  post: DisplayPost;
  replies: TimelinePostNode[];
  activityAt: string;
};

export type TimelineItem = {
  id: string;
  node: TimelinePostNode;
  threadRoot: DisplayPost;
  repost?: IncludedRepost | null;
  threadUrl?: string;
};

export function optimisticReplyCount(
  parent: Pick<IncludedPost, "authorDid" | "replyCount">,
  viewerDid: string,
) {
  return parent.authorDid === viewerDid ? parent.replyCount + 1 : undefined;
}

function toDisplayPost(post: IncludedPost): DisplayPost {
  return {
    ...post,
    images: [...(post.postImagesViaPost ?? [])].sort((a, b) => a.position - b.position),
    quote: post.quotedPost ? toDisplayPost(post.quotedPost) : undefined,
    like: post.likesViaSubjectPost?.[0],
    repost: post.repostsViaSubjectPost?.[0],
  };
}

export function buildThread(rootId: string, posts: IncludedPost[]) {
  const unique = new Map(posts.map((post) => [post.id, toDisplayPost(post)]));
  const nodes = new Map<string, TimelinePostNode>(
    [...unique].map(([id, post]) => [id, { post, replies: [], activityAt: post.createdAt }]),
  );
  const roots: TimelinePostNode[] = [];
  for (const node of nodes.values()) {
    const parent = node.post.replyParentId ? nodes.get(node.post.replyParentId) : undefined;
    if (parent) parent.replies.push(node);
    else roots.push(node);
  }
  const root = nodes.get(rootId) ?? roots[0];
  if (!root) return undefined;
  const sort = (node: TimelinePostNode, authorDid: string): string => {
    node.replies.sort((a, b) =>
      Number(b.post.authorDid === authorDid) - Number(a.post.authorDid === authorDid)
      || a.post.createdAt.localeCompare(b.post.createdAt)
      || a.post.id.localeCompare(b.post.id));
    for (const reply of node.replies) {
      const activityAt = sort(reply, authorDid);
      if (activityAt > node.activityAt) node.activityAt = activityAt;
    }
    return node.activityAt;
  };
  sort(root, root.post.authorDid);
  return root;
}

export function buildTimeline(rows: TimelineEntryView[]) {
  const items: TimelineItem[] = [];
  const seenThreads = new Set<string>();
  const orderedRows = [...rows].sort((a, b) =>
    b.sortAt.localeCompare(a.sortAt) || a.id.localeCompare(b.id));
  for (const row of orderedRows) {
    if (!row.post) continue;
    if (!row.repostId && seenThreads.has(row.threadRootId)) continue;
    if (!row.repostId) seenThreads.add(row.threadRootId);
    const repostedReply = Boolean(row.repostId && row.post.replyParentId);
    const threadPosts = (row.threadRoot?.threadEntriesViaRootPost ?? [])
      .flatMap((entry) => entry.state === "post" && entry.post ? [entry.post] : []);
    if (!threadPosts.some((post) => post.id === row.post!.id)) threadPosts.push(row.post);
    if (row.threadRoot && !threadPosts.some((post) => post.id === row.threadRoot!.id)) threadPosts.push(row.threadRoot);
    const node = repostedReply
      ? buildThread(row.post.id, [row.post])
      : buildThread(row.threadRootId, threadPosts) ?? buildThread(row.post.id, [row.post]);
    if (node) items.push({
      id: row.repostId ? row.id : `thread:${row.threadRootId}`,
      node,
      threadRoot: row.threadRoot ? toDisplayPost(row.threadRoot) : node.post,
      repost: row.repost,
      threadUrl: repostedReply
        ? `https://bsky.app/profile/${row.post.authorDid}/post/${row.post.uri.split("/").at(-1)}`
        : undefined,
    });
  }
  return items;
}
