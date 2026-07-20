import { createHash } from "node:crypto";
import { appId } from "../app-id.js";
import { formatObjectId, objectIdKey } from "../object-id.js";

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
  facets?: Array<{
    index?: { byteStart?: number; byteEnd?: number };
    features?: Array<{ $type?: string; uri?: string }>;
  }>;
  reply?: { parent?: StrongRef; root?: StrongRef };
};

export type PostImageView = {
  thumb?: string;
  fullsize?: string;
  alt?: string;
  aspectRatio?: { width?: number; height?: number };
};

type EmbeddedRecordView = {
  $type?: string;
  uri?: string;
  cid?: string;
  author?: ProfileView;
  value?: PostRecord;
  indexedAt?: string;
  embeds?: PostEmbedView[];
};

type PostEmbedView = {
  images?: PostImageView[];
  media?: { images?: PostImageView[] };
  record?: EmbeddedRecordView & { record?: EmbeddedRecordView };
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
  embed?: PostEmbedView;
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

export function stableObjectId(
  namespace: string,
  value: string,
  applicationId = appId,
) {
  return formatObjectId(createHash("sha256").update(objectIdKey(applicationId, namespace, value)).digest());
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

function normalizePostBase(post: PostView | undefined) {
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
  const linkFacets = (record.facets ?? []).flatMap((facet) => {
    const link = facet.features?.find(
      (feature) => feature.$type === "app.bsky.richtext.facet#link" && feature.uri,
    );
    const byteStart = facet.index?.byteStart;
    const byteEnd = facet.index?.byteEnd;
    return link && Number.isInteger(byteStart) && Number.isInteger(byteEnd)
      ? [{ byteStart: byteStart!, byteEnd: byteEnd!, uri: link.uri! }]
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
export type NormalizedPostBundle = Omit<BasePostBundle, "post"> & {
  post: BasePostBundle["post"] & { quotedPostId?: string };
  quote?: BasePostBundle;
};

function quotedPostView(embed: PostEmbedView | undefined): PostView | undefined {
  const embedded = embed?.record?.record ?? embed?.record;
  if (!embedded?.uri || !embedded.cid || !embedded.author?.did || !embedded.value) return undefined;
  return {
    uri: embedded.uri,
    cid: embedded.cid,
    author: embedded.author,
    record: embedded.value,
    indexedAt: embedded.indexedAt,
    embed: embedded.embeds?.[0],
  };
}

export function normalizePost(post: PostView | undefined): NormalizedPostBundle | null {
  const normalized = normalizePostBase(post);
  if (!normalized) return null;
  const quote = normalizePostBase(quotedPostView(post?.embed));
  return {
    ...normalized,
    post: {
      ...normalized.post,
      quotedPostId: quote?.post.id,
    },
    quote: quote ?? undefined,
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
  const context = [item.reply?.root, item.reply?.parent]
    .map(normalizePost)
    .filter((value): value is NonNullable<ReturnType<typeof normalizePost>> => value !== null);
  const threadRoot = context.find((bundle) => bundle.post.id === threadRootId);
  const timelineEntry = {
    id: stableObjectId("timeline-entry", `${ownerDid}:${eventKey}`),
    ownerDid,
    postId: post.id,
    threadRootId,
    repostId: reasonRepostId,
    sortAt: reason?.indexedAt ?? threadRoot?.post.indexedAt ?? post.indexedAt,
    active: true,
  };

  return {
    profiles,
    post,
    images: normalizedPost.images,
    likes,
    reposts: [...reposts.values()],
    timelineEntry,
    context,
    quote: normalizedPost.quote,
  };
}
