export type ProfileView = {
  did?: string;
  handle?: string;
  displayName?: string;
  avatar?: string;
};

export type ImageView = {
  id: string;
  postCid: string;
  position: number;
  thumb: string;
  fullsize: string;
  alt: string;
  aspectWidth?: number | null;
  aspectHeight?: number | null;
};

export type ReactionView = {
  id: string;
  uri?: string | null;
  active: boolean;
};

export type IncludedPost = {
  id: string;
  uri: string;
  cid?: string | null;
  authorDid: string;
  text: string;
  facetsJson?: string | null;
  createdAt: string;
  indexedAt: string;
  replyParentId?: string | null;
  replyRootId?: string | null;
  replyCount: number;
  likeCount: number;
  repostCount: number;
  state: string;
  authorProfile?: ProfileView | null;
  postImagesViaPost?: ImageView[];
  likesViaSubjectPost?: ReactionView[];
  repostsViaSubjectPost?: ReactionView[];
};

type IncludedThreadEntry = {
  id: string;
  state: string;
  sortOrder: number;
  post?: IncludedPost | null;
};

type IncludedThreadRoot = IncludedPost & {
  threadEntriesViaRootPost?: IncludedThreadEntry[];
};

export type IncludedRepost = ReactionView & {
  actorDid: string;
  actorProfile?: ProfileView | null;
};

export type TimelineEntryView = {
  id: string;
  ownerDid: string;
  postId: string;
  threadRootId: string;
  repostId?: string | null;
  sortAt: string;
  post?: IncludedPost | null;
  threadRoot?: IncludedThreadRoot | null;
  repost?: IncludedRepost | null;
};

export type DisplayPost = IncludedPost & {
  images: ImageView[];
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
  repost?: IncludedRepost | null;
};

export type PendingOperationView = {
  id: string;
  operationId: string;
  ownerDid: string;
  kind: string;
  target: string;
  rkey: string;
  payload: string;
  state: string;
  error?: string | null;
  createdAt: string;
};

function toDisplayPost(post: IncludedPost): DisplayPost {
  return {
    ...post,
    images: [...(post.postImagesViaPost ?? [])].sort((a, b) => a.position - b.position),
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
      || a.post.createdAt.localeCompare(b.post.createdAt));
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
  for (const row of rows) {
    if (!row.post) continue;
    if (!row.repostId && seenThreads.has(row.threadRootId)) continue;
    if (!row.repostId) seenThreads.add(row.threadRootId);
    const threadPosts = (row.threadRoot?.threadEntriesViaRootPost ?? [])
      .flatMap((entry) => entry.state === "post" && entry.post ? [entry.post] : []);
    if (!threadPosts.some((post) => post.id === row.post!.id)) threadPosts.push(row.post);
    if (row.threadRoot && !threadPosts.some((post) => post.id === row.threadRoot!.id)) threadPosts.push(row.threadRoot);
    const node = buildThread(row.threadRootId, threadPosts) ?? buildThread(row.post.id, [row.post]);
    if (node) items.push({ id: row.id, node, repost: row.repost });
  }
  return items;
}
