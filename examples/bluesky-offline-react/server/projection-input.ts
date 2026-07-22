import { createHash } from "node:crypto";
import {
  AppBskyActorDefs,
  AppBskyEmbedImages,
  AppBskyEmbedRecord,
  AppBskyEmbedRecordWithMedia,
  AppBskyFeedDefs,
  AppBskyFeedPost,
  AppBskyRichtextFacet,
} from "@atproto/api";
import { formatObjectId, jazzAppId, objectIdKey } from "../shared/identifiers.js";

export type ThreadNode =
  | AppBskyFeedDefs.ThreadViewPost
  | AppBskyFeedDefs.NotFoundPost
  | AppBskyFeedDefs.BlockedPost
  | { $type: string };

export function stableObjectId(namespace: string, value: string, applicationId = jazzAppId) {
  return formatObjectId(
    createHash("sha256")
      .update(objectIdKey(applicationId, namespace, value))
      .digest(),
  );
}

function profileRow(
  profile: AppBskyActorDefs.ProfileViewBasic | AppBskyActorDefs.ProfileViewDetailed | undefined,
  indexedAt: string,
) {
  if (!profile?.did) return undefined;
  return {
    id: stableObjectId("bluesky-profile", profile.did),
    did: profile.did,
    handle: profile.handle,
    displayName: profile.displayName,
    description: "description" in profile ? profile.description : undefined,
    avatar: profile.avatar,
    indexedAt,
  };
}

function postRecord(post: AppBskyFeedDefs.PostView | undefined) {
  if (!post) return undefined;
  const result = AppBskyFeedPost.validateRecord({
    ...post.record,
    $type: "app.bsky.feed.post",
  });
  return result.success ? result.value : undefined;
}

function postImages(embed: AppBskyFeedDefs.PostView["embed"]) {
  if (AppBskyEmbedImages.isView(embed)) return embed.images;
  if (AppBskyEmbedRecordWithMedia.isView(embed) && AppBskyEmbedImages.isView(embed.media)) {
    return embed.media.images;
  }
  return [];
}

function normalizePostBase(post: AppBskyFeedDefs.PostView | undefined) {
  const record = postRecord(post);
  if (!post || !record) return null;
  const { uri, cid } = post;
  const authorDid = post.author.did;
  const indexedAt = post.indexedAt || record.createdAt;
  const id = stableObjectId("bluesky-post", uri);
  const authorProfile = profileRow(post.author, indexedAt);
  const replyParentId = record.reply?.parent?.uri
    ? stableObjectId("bluesky-post", record.reply.parent.uri)
    : undefined;
  const replyRootId = record.reply?.root?.uri
    ? stableObjectId("bluesky-post", record.reply.root.uri)
    : undefined;
  const images = postImages(post.embed)
    .slice(0, 4)
    .map((image, position) => ({
      id: stableObjectId("bluesky-post-image", `${uri}:${position}`),
      postId: id,
      postCid: cid,
      position,
      thumb: image.thumb,
      fullsize: image.fullsize,
      alt: image.alt,
      aspectWidth: image.aspectRatio?.width,
      aspectHeight: image.aspectRatio?.height,
    }));
  const linkFacets = (record.facets ?? []).flatMap((facet) => {
    const link = facet.features
      .map(AppBskyRichtextFacet.validateLink)
      .find((result) => result.success);
    return link
      ? [{ byteStart: facet.index.byteStart, byteEnd: facet.index.byteEnd, uri: link.value.uri }]
      : [];
  });

  return {
    profile: authorProfile,
    post: {
      id,
      uri,
      cid,
      authorDid,
      authorProfileId: stableObjectId("bluesky-profile", authorDid),
      text: record.text,
      facetsJson: linkFacets.length > 0 ? JSON.stringify(linkFacets) : undefined,
      createdAt: record.createdAt,
      indexedAt,
      replyParentId,
      replyRootId,
      replyCount: post.replyCount ?? 0,
      likeCount: post.likeCount ?? 0,
      repostCount: post.repostCount ?? 0,
      state: "synced",
    },
    images,
  };
}

type BasePostBundle = NonNullable<ReturnType<typeof normalizePostBase>>;
type NormalizedPostBundle = Omit<BasePostBundle, "post"> & {
  post: BasePostBundle["post"] & { quotedPostId?: string };
  quote?: BasePostBundle;
};

function quotedPost(
  embed: AppBskyFeedDefs.PostView["embed"],
): AppBskyFeedDefs.PostView | undefined {
  const recordEmbed = AppBskyEmbedRecordWithMedia.isView(embed)
    ? embed.record
    : AppBskyEmbedRecord.isView(embed)
      ? embed
      : undefined;
  const embedded = recordEmbed?.record;
  if (!AppBskyEmbedRecord.isViewRecord(embedded)) return undefined;
  return {
    uri: embedded.uri,
    cid: embedded.cid,
    author: embedded.author,
    record: embedded.value,
    indexedAt: embedded.indexedAt,
    embed: embedded.embeds?.[0],
  };
}

export function normalizePost(
  post: AppBskyFeedDefs.PostView | undefined,
): NormalizedPostBundle | null {
  const normalized = normalizePostBase(post);
  if (!normalized) return null;
  const quote = normalizePostBase(quotedPost(post?.embed));
  return {
    ...normalized,
    post: {
      ...normalized.post,
      quotedPostId: quote?.post.id,
    },
    quote: quote ?? undefined,
  };
}

export function normalizeTimelineItem(ownerDid: string, item: AppBskyFeedDefs.FeedViewPost) {
  const normalizedPost = normalizePost(item.post);
  if (!normalizedPost) return null;
  const { post } = normalizedPost;
  const reason = AppBskyFeedDefs.isReasonRepost(item.reason) ? item.reason : undefined;
  const reposterProfile = reason?.indexedAt ? profileRow(reason.by, reason.indexedAt) : undefined;
  const repost:
    | {
        id: string;
        uri?: string;
        cid?: string;
        actorDid: string;
        actorProfileId: string;
        subjectPostId: string;
        createdAt: string;
        active: boolean;
      }
    | undefined =
    reason?.by?.did && reason.indexedAt
      ? {
          id: stableObjectId("bluesky-repost", `${reason.by.did}:${post.uri}`),
          uri: reason.uri,
          cid: reason.cid,
          actorDid: reason.by.did,
          actorProfileId: stableObjectId("bluesky-profile", reason.by.did),
          subjectPostId: post.id,
          createdAt: reason.indexedAt,
          active: true,
        }
      : undefined;
  const eventKey =
    reason?.uri ??
    (reason?.by?.did && reason.indexedAt
      ? `repost:${reason.by.did}:${post.uri}:${reason.indexedAt}`
      : post.uri);
  const threadRootId = post.replyRootId ?? post.id;
  const context = [item.reply?.root, item.reply?.parent]
    .filter(AppBskyFeedDefs.isPostView)
    .map(normalizePost)
    .filter((value): value is NonNullable<ReturnType<typeof normalizePost>> => value !== null);
  const threadRoot = context.find((bundle) => bundle.post.id === threadRootId);
  const timelineEntry = {
    id: stableObjectId("timeline-entry", `${ownerDid}:${eventKey}`),
    ownerDid,
    postId: post.id,
    threadRootId,
    repostId: repost?.id,
    sortAt: reason?.indexedAt ?? threadRoot?.post.indexedAt ?? post.indexedAt,
    active: true,
  };

  return {
    postBundle: normalizedPost,
    reposterProfile,
    repost,
    timelineEntry,
    context,
  };
}

type FlatThreadEntry = {
  post?: AppBskyFeedDefs.PostView;
  postId: string;
  parentPostId?: string;
  sortOrder: number;
  state: "post" | "blocked" | "not-found";
};

type FlatThread = {
  rootPostId: string;
  entries: FlatThreadEntry[];
};

export function flattenThread(
  requestedUri: string,
  thread: ThreadNode,
  toPostId: (uri: string) => string = (uri) => uri,
): FlatThread {
  const ancestors: ThreadNode[] = [];
  for (
    let node: ThreadNode | undefined = thread;
    node;
    node = AppBskyFeedDefs.isThreadViewPost(node) ? node.parent : undefined
  ) {
    ancestors.unshift(node);
  }

  const selectedPost = AppBskyFeedDefs.isThreadViewPost(thread) ? thread.post : undefined;
  const selectedUri =
    selectedPost?.uri ?? ("uri" in thread ? thread.uri : undefined) ?? requestedUri;
  const rootRecord = postRecord(selectedPost);
  const firstAncestor = ancestors[0];
  const firstPost = AppBskyFeedDefs.isThreadViewPost(firstAncestor)
    ? firstAncestor.post
    : undefined;
  const rootUri =
    rootRecord?.reply?.root.uri ??
    firstPost?.uri ??
    (firstAncestor && "uri" in firstAncestor ? firstAncestor.uri : undefined) ??
    requestedUri;
  const entries: FlatThreadEntry[] = [];
  const seen = new Set<string>();

  const addNode = (node: ThreadNode, fallbackParentId?: string) => {
    const post = AppBskyFeedDefs.isThreadViewPost(node) ? node.post : undefined;
    const uri = post?.uri ?? ("uri" in node ? node.uri : undefined);
    if (!uri) return undefined;
    const postId = toPostId(uri);
    if (seen.has(postId)) return postId;
    seen.add(postId);
    const parentUri = postRecord(post)?.reply?.parent.uri;
    entries.push({
      post,
      postId,
      parentPostId: parentUri ? toPostId(parentUri) : fallbackParentId,
      sortOrder: entries.length,
      state: post ? "post" : AppBskyFeedDefs.isBlockedPost(node) ? "blocked" : "not-found",
    });
    return postId;
  };

  let parentId: string | undefined;
  for (const ancestor of ancestors) {
    parentId = addNode(ancestor, parentId) ?? parentId;
  }

  const addReplies = (nodes: ThreadNode[] | undefined, replyParentId: string) => {
    for (const node of nodes ?? []) {
      const postId = addNode(node, replyParentId);
      if (postId && AppBskyFeedDefs.isThreadViewPost(node)) addReplies(node.replies, postId);
    }
  };
  addReplies(
    AppBskyFeedDefs.isThreadViewPost(thread) ? thread.replies : undefined,
    toPostId(selectedUri),
  );

  return { rootPostId: toPostId(rootUri), entries };
}

export type PostBundle = NonNullable<ReturnType<typeof normalizePost>>;
export type NormalizedPost = PostBundle["post"];
