import { createHash } from "node:crypto";

export type ProfileView = {
  did?: string;
  handle?: string;
  displayName?: string;
  description?: string;
  avatar?: string;
};

export type StrongRef = { uri?: string; cid?: string };

export type PostRecord = {
  text?: string;
  createdAt?: string;
  reply?: { parent?: StrongRef; root?: StrongRef };
};

export type PostImageView = {
  thumb?: string;
  fullsize?: string;
  alt?: string;
  aspectRatio?: { width?: number; height?: number };
};

export type PostView = {
  uri?: string;
  cid?: string;
  author?: ProfileView;
  record?: PostRecord;
  indexedAt?: string;
  replyCount?: number;
  likeCount?: number;
  repostCount?: number;
  viewer?: { like?: string; repost?: string };
  embed?: { images?: PostImageView[]; media?: { images?: PostImageView[] } };
};

export type RepostReason = {
  $type?: string;
  by?: ProfileView;
  uri?: string;
  cid?: string;
  indexedAt?: string;
};

export type FeedViewPost = {
  post?: PostView;
  reply?: { root?: PostView; parent?: PostView };
  reason?: RepostReason;
};

export function stableObjectId(namespace: string, value: string) {
  const bytes = createHash("sha256").update(`${namespace}:${value}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function profileRow(profile: ProfileView | undefined, indexedAt: string) {
  if (!profile?.did) return undefined;
  return {
    id: stableObjectId("bluesky-profile", profile.did),
    did: profile.did,
    handle: profile.handle,
    displayName: profile.displayName,
    description: profile.description,
    avatar: profile.avatar,
    indexedAt,
  };
}

export function normalizePost(post: PostView | undefined) {
  const uri = post?.uri;
  const cid = post?.cid;
  const authorDid = post?.author?.did;
  const record = post?.record;
  if (!uri || !cid || !authorDid || typeof record?.text !== "string" || !record.createdAt) return null;
  const indexedAt = post.indexedAt ?? record.createdAt;
  const id = stableObjectId("bluesky-post", uri);
  const authorProfile = profileRow(post.author, indexedAt);
  const replyParentId = record.reply?.parent?.uri
    ? stableObjectId("bluesky-post", record.reply.parent.uri)
    : undefined;
  const replyRootId = record.reply?.root?.uri
    ? stableObjectId("bluesky-post", record.reply.root.uri)
    : undefined;
  const images = (post.embed?.images ?? post.embed?.media?.images ?? [])
    .filter((image) => image.thumb && image.fullsize)
    .slice(0, 4)
    .map((image, position) => ({
      id: stableObjectId("bluesky-post-image", `${uri}:${position}`),
      postId: id,
      postCid: cid,
      position,
      thumb: image.thumb!,
      fullsize: image.fullsize!,
      alt: image.alt ?? "",
      aspectWidth: image.aspectRatio?.width,
      aspectHeight: image.aspectRatio?.height,
    }));

  return {
    profile: authorProfile,
    post: {
      id,
      uri,
      cid,
      authorDid,
      authorProfileId: stableObjectId("bluesky-profile", authorDid),
      text: record.text,
      createdAt: record.createdAt,
      createdAtMs: Math.floor(Date.parse(record.createdAt) / 1_000),
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

export function normalizeTimelineItem(ownerDid: string, item: FeedViewPost) {
  const normalizedPost = normalizePost(item.post);
  if (!normalizedPost) return null;
  const { post } = normalizedPost;
  const profiles = normalizedPost.profile ? [normalizedPost.profile] : [];
  const likes = item.post?.viewer?.like
    ? [{
        id: stableObjectId("bluesky-like", `${ownerDid}:${post.uri}`),
        uri: item.post.viewer.like,
        actorDid: ownerDid,
        subjectPostId: post.id,
        createdAt: post.indexedAt,
        active: true,
      }]
    : [];
  const reposts = new Map<string, {
    id: string;
    uri?: string;
    cid?: string;
    actorDid: string;
    actorProfileId: string;
    subjectPostId: string;
    createdAt: string;
    active: boolean;
  }>();
  const reason = item.reason?.$type === "app.bsky.feed.defs#reasonRepost" ? item.reason : undefined;
  let reasonRepostId: string | undefined;
  if (reason?.by?.did && reason.indexedAt) {
    const reposterProfile = profileRow(reason.by, reason.indexedAt);
    if (reposterProfile) profiles.push(reposterProfile);
    reasonRepostId = stableObjectId("bluesky-repost", `${reason.by.did}:${post.uri}`);
    reposts.set(reasonRepostId, {
      id: reasonRepostId,
      uri: reason.uri,
      cid: reason.cid,
      actorDid: reason.by.did,
      actorProfileId: stableObjectId("bluesky-profile", reason.by.did),
      subjectPostId: post.id,
      createdAt: reason.indexedAt,
      active: true,
    });
  }
  if (item.post?.viewer?.repost) {
    const id = stableObjectId("bluesky-repost", `${ownerDid}:${post.uri}`);
    reposts.set(id, {
      id,
      uri: item.post.viewer.repost,
      actorDid: ownerDid,
      actorProfileId: stableObjectId("bluesky-profile", ownerDid),
      subjectPostId: post.id,
      createdAt: reposts.get(id)?.createdAt ?? post.indexedAt,
      active: true,
    });
  }
  const eventKey = reason?.uri
    ?? (reason?.by?.did && reason.indexedAt
      ? `repost:${reason.by.did}:${post.uri}:${reason.indexedAt}`
      : post.uri);
  const threadRootId = post.replyRootId ?? post.id;
  const timelineEntry = {
    id: stableObjectId("timeline-entry", `${ownerDid}:${eventKey}`),
    ownerDid,
    postId: post.id,
    threadRootId,
    repostId: reasonRepostId,
    sortAt: reason?.indexedAt ?? post.indexedAt,
    active: true,
  };

  return {
    profiles,
    post,
    images: normalizedPost.images,
    likes,
    reposts: [...reposts.values()],
    timelineEntry,
    context: [item.reply?.root, item.reply?.parent]
      .map(normalizePost)
      .filter((value): value is NonNullable<ReturnType<typeof normalizePost>> => value !== null),
  };
}
