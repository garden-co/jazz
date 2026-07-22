import { schema as s } from "jazz-tools";
import { app } from "../../schema.js";

function includedPost(ownerDid: string) {
  const quotedPost = app.posts
    .include({ authorProfile: true, postImagesViaPost: true })
    .requireIncludes();
  return app.posts
    .include({
      authorProfile: true,
      postImagesViaPost: true,
      quotedPost,
      likesViaSubjectPost: app.likes.where({ actorDid: { eq: ownerDid } }),
      repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: ownerDid } }),
    })
    .requireIncludes();
}

export function timelineQuery(ownerDid: string) {
  const post = includedPost(ownerDid);
  return app.timelineEntries
    .where({ ownerDid: { eq: ownerDid }, active: { eq: true } })
    .orderBy("sortAt", "desc")
    .include({
      post,
      repost: app.reposts.include({ actorProfile: true }).requireIncludes(),
      threadRoot: app.posts
        .include({
          authorProfile: true,
          postImagesViaPost: true,
          quotedPost: app.posts
            .include({ authorProfile: true, postImagesViaPost: true })
            .requireIncludes(),
          likesViaSubjectPost: app.likes.where({ actorDid: { eq: ownerDid } }),
          repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: ownerDid } }),
          threadEntriesViaRootPost: app.threadEntries
            .orderBy("sortOrder", "asc")
            .include({ post })
            .requireIncludes(),
        })
        .requireIncludes(),
    })
    .requireIncludes();
}

type PostRow = s.RowOf<typeof app.posts>;
type ProfileView = s.RowOf<typeof app.profiles>;
type RepostRow = s.RowOf<typeof app.reposts>;

export type ThreadPostView = PostRow;
export type TimelineQueryRow = s.RowOf<ReturnType<typeof timelineQuery>>;
export type TimelineEntryView = s.RowOf<typeof app.timelineEntries>;
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

function addPostRelations(
  post: TimelineQueryRow["post"],
  relations: {
    posts: Map<string, PostRow>;
    profiles: Map<string, ProfileView>;
    images: Map<string, ImageView>;
    likes: Map<string, s.RowOf<typeof app.likes>>;
    reposts: Map<string, s.RowOf<typeof app.reposts>>;
  },
) {
  relations.posts.set(post.id, post);
  relations.profiles.set(post.authorProfile.id, post.authorProfile);
  for (const image of post.postImagesViaPost) relations.images.set(image.id, image);
  for (const like of post.likesViaSubjectPost) relations.likes.set(like.id, like);
  for (const repost of post.repostsViaSubjectPost) relations.reposts.set(repost.id, repost);
  if (post.quotedPost) {
    relations.posts.set(post.quotedPost.id, post.quotedPost);
    relations.profiles.set(post.quotedPost.authorProfile.id, post.quotedPost.authorProfile);
    for (const image of post.quotedPost.postImagesViaPost) relations.images.set(image.id, image);
  }
}

export function timelineRelations(rows: TimelineQueryRow[], viewerDid: string): TimelineRelations {
  const relations = {
    posts: new Map<string, PostRow>(),
    profiles: new Map<string, ProfileView>(),
    images: new Map<string, ImageView>(),
    likes: new Map<string, s.RowOf<typeof app.likes>>(),
    reposts: new Map<string, s.RowOf<typeof app.reposts>>(),
  };
  for (const row of rows) {
    addPostRelations(row.post, relations);
    addPostRelations(row.threadRoot, relations);
    for (const entry of row.threadRoot.threadEntriesViaRootPost) {
      addPostRelations(entry.post, relations);
    }
    if (row.repost) {
      relations.reposts.set(row.repost.id, row.repost);
      relations.profiles.set(row.repost.actorProfile.id, row.repost.actorProfile);
    }
  }
  return {
    viewerDid,
    posts: [...relations.posts.values()],
    profiles: [...relations.profiles.values()],
    images: [...relations.images.values()],
    likes: [...relations.likes.values()],
    reposts: [...relations.reposts.values()],
  };
}

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
  relations: RelationIndex,
  seen = new Set<string>(),
): DisplayPost {
  const quote =
    post.quotedPostId && !seen.has(post.quotedPostId)
      ? relations.posts.get(post.quotedPostId)
      : undefined;
  return {
    ...post,
    authorProfile: relations.profiles.get(post.authorProfileId),
    images: relations.images.get(post.id) ?? [],
    quote: quote ? toDisplayPost(quote, relations, new Set(seen).add(post.id)) : undefined,
    like: relations.likes.get(post.id),
    repost: relations.reposts.get(`${relations.viewerDid}:${post.id}`),
  };
}

type RelationIndex = {
  viewerDid?: string;
  posts: Map<string, PostRow>;
  profiles: Map<string, ProfileView>;
  images: Map<string, ImageView[]>;
  likes: Map<string, s.RowOf<typeof app.likes>>;
  reposts: Map<string, s.RowOf<typeof app.reposts>>;
  repostsById: Map<string, s.RowOf<typeof app.reposts>>;
};

function indexRelations(relations: TimelineRelations): RelationIndex {
  const images = new Map<string, ImageView[]>();
  for (const image of relations.images) {
    const postImages = images.get(image.postId) ?? [];
    postImages.push(image);
    postImages.sort((a, b) => a.position - b.position);
    images.set(image.postId, postImages);
  }
  return {
    viewerDid: relations.viewerDid,
    posts: new Map(relations.posts.map((post) => [post.id, post])),
    profiles: new Map(relations.profiles.map((profile) => [profile.id, profile])),
    images,
    likes: new Map(relations.likes.map((like) => [like.subjectPostId, like])),
    reposts: new Map(
      relations.reposts.map((repost) => [`${repost.actorDid}:${repost.subjectPostId}`, repost]),
    ),
    repostsById: new Map(relations.reposts.map((repost) => [repost.id, repost])),
  };
}

export function buildThread(rootId: string, posts: IncludedPost[], relations = emptyRelations) {
  return buildThreadFromIndex(rootId, posts, indexRelations(relations));
}

function buildThreadFromIndex(rootId: string, posts: IncludedPost[], relations: RelationIndex) {
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
  const index = indexRelations(relations);
  const items: TimelineItem[] = [];
  const seenPostUris = new Set<string>();
  const seenThreads = new Set<string>();
  const orderedRows = [...rows].sort(
    (a, b) => b.sortAt.localeCompare(a.sortAt) || a.id.localeCompare(b.id),
  );
  for (const row of orderedRows) {
    const post = index.posts.get(row.postId);
    const threadRoot = index.posts.get(row.threadRootId);
    if (!post || !threadRoot || seenPostUris.has(post.uri)) continue;
    seenPostUris.add(post.uri);
    if (!row.repostId && seenThreads.has(row.threadRootId)) continue;
    if (!row.repostId) seenThreads.add(row.threadRootId);
    const repostedReply = Boolean(row.repostId && post.replyParentId);
    const node = repostedReply
      ? buildThreadFromIndex(post.id, [post], index)
      : (buildThreadFromIndex(row.threadRootId, [threadRoot, post], index) ??
        buildThreadFromIndex(post.id, [post], index));
    const repost = row.repostId ? index.repostsById.get(row.repostId) : undefined;
    if (node)
      items.push({
        id: row.repostId ? row.id : `thread:${row.threadRootId}`,
        ownerDid: row.ownerDid,
        node,
        threadRoot: toDisplayPost(threadRoot, index),
        repost: repost && {
          ...repost,
          actorProfile: index.profiles.get(repost.actorProfileId),
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
  const index = indexRelations(relations);
  const posts = threadPosts.map((post) => toDisplayPost(post, index));
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
