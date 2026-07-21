import {
  Agent,
  AppBskyEmbedImages,
  AppBskyEmbedRecord,
  AppBskyEmbedRecordWithMedia,
  AppBskyFeedDefs,
  AppBskyFeedPost,
  AppBskyRichtextFacet,
  XRPCError,
} from "@atproto/api";
import { parseAtUri } from "../at-uri.js";
import type { OAuthSession } from "./auth.js";

export type SessionFetcher = Pick<OAuthSession, "fetchHandler">;

export type ProfileView = {
  did?: string;
  handle?: string;
  displayName?: string;
  description?: string;
  avatar?: string;
};

type StrongRef = { uri?: string; cid?: string };

export type PostRecord = {
  text?: string;
  createdAt?: string;
  facets?: Array<{
    index?: { byteStart?: number; byteEnd?: number };
    features?: Array<{ $type?: string; uri?: string }>;
  }>;
  reply?: { parent?: StrongRef; root?: StrongRef };
};

type PostImageView = {
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

export type PostEmbedView = {
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

type RepostReason = {
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

export type ThreadViewNode = {
  $type?: string;
  uri?: string;
  blocked?: boolean;
  notFound?: boolean;
  post?: PostView;
  parent?: ThreadViewNode;
  replies?: ThreadViewNode[];
};

const timelinePageSize = 20;

export class OperationError extends Error {
  constructor(
    message: string,
    readonly status: 400 | 502,
  ) {
    super(message);
  }
}

function toPostRecord(value: Record<string, unknown>): PostRecord | undefined {
  const result = AppBskyFeedPost.validateRecord(value);
  if (!result.success) return undefined;
  const record = result.value;
  return {
    text: record.text,
    createdAt: record.createdAt,
    reply: record.reply ? { parent: record.reply.parent, root: record.reply.root } : undefined,
    facets: record.facets?.map((facet) => ({
      index: facet.index,
      features: facet.features.map((feature) => {
        const link = AppBskyRichtextFacet.validateLink(feature);
        return {
          $type: feature.$type,
          ...(link.success ? { uri: link.value.uri } : {}),
        };
      }),
    })),
  };
}

function toEmbeddedRecord(record: AppBskyEmbedRecord.ViewRecord) {
  const value = toPostRecord(record.value);
  const embeds = record.embeds?.flatMap((embed) => {
    const converted = toPostEmbed(embed);
    return converted ? [converted] : [];
  });
  return {
    uri: record.uri,
    cid: record.cid,
    author: record.author,
    value,
    indexedAt: record.indexedAt,
    embeds,
  };
}

function toRecordEmbed(embed: AppBskyEmbedRecord.View) {
  return AppBskyEmbedRecord.isViewRecord(embed.record) ? toEmbeddedRecord(embed.record) : undefined;
}

function toPostEmbed(embed: AppBskyFeedDefs.PostView["embed"]): PostView["embed"] {
  if (AppBskyEmbedImages.isView(embed)) return { images: embed.images };
  if (AppBskyEmbedRecord.isView(embed)) {
    const record = toRecordEmbed(embed);
    return record ? { record } : undefined;
  }
  if (AppBskyEmbedRecordWithMedia.isView(embed)) {
    const record = toRecordEmbed(embed.record);
    const media = AppBskyEmbedImages.isView(embed.media)
      ? { images: embed.media.images }
      : undefined;
    return record || media ? { record, media } : undefined;
  }
  return undefined;
}

function toPostView(post: AppBskyFeedDefs.PostView): PostView {
  const record = toPostRecord(post.record);
  const embed = toPostEmbed(post.embed);
  return {
    uri: post.uri,
    cid: post.cid,
    author: post.author,
    indexedAt: post.indexedAt,
    replyCount: post.replyCount,
    likeCount: post.likeCount,
    repostCount: post.repostCount,
    viewer: post.viewer,
    ...(record ? { record } : {}),
    ...(embed ? { embed } : {}),
  };
}

function toFeedViewPost(item: AppBskyFeedDefs.FeedViewPost): FeedViewPost {
  const reason = AppBskyFeedDefs.isReasonRepost(item.reason) ? item.reason : undefined;
  const root =
    item.reply && AppBskyFeedDefs.isPostView(item.reply.root)
      ? toPostView(item.reply.root)
      : undefined;
  const parent =
    item.reply && AppBskyFeedDefs.isPostView(item.reply.parent)
      ? toPostView(item.reply.parent)
      : undefined;
  return {
    post: toPostView(item.post),
    ...(root || parent ? { reply: { root, parent } } : {}),
    ...(reason ? { reason } : {}),
  };
}

function toThreadViewNode(
  node:
    | AppBskyFeedDefs.ThreadViewPost
    | AppBskyFeedDefs.NotFoundPost
    | AppBskyFeedDefs.BlockedPost
    | { $type: string },
): ThreadViewNode {
  if (AppBskyFeedDefs.isThreadViewPost(node)) {
    return {
      $type: node.$type,
      post: toPostView(node.post),
      parent: node.parent ? toThreadViewNode(node.parent) : undefined,
      replies: node.replies?.map(toThreadViewNode),
    };
  }
  if (AppBskyFeedDefs.isBlockedPost(node)) {
    return { $type: node.$type, uri: node.uri, blocked: true };
  }
  if (AppBskyFeedDefs.isNotFoundPost(node)) {
    return { $type: node.$type, uri: node.uri, notFound: true };
  }
  return { $type: node.$type };
}

export async function fetchTimelineFeed(session: SessionFetcher, cursor?: string) {
  const response = await new Agent(session).getTimeline({
    limit: timelinePageSize,
    ...(cursor ? { cursor } : {}),
  });
  return {
    cursor: response.data.cursor,
    feed: response.data.feed.map(toFeedViewPost),
  };
}

export async function fetchViewerPosts(session: SessionFetcher, uris: string[]) {
  const response = await new Agent(session).getPosts({ uris });
  return response.data.posts.map(toPostView);
}

export async function fetchPostThread(session: SessionFetcher, uri: string) {
  const response = await new Agent(session).getPostThread({ uri, depth: 100, parentHeight: 100 });
  return toThreadViewNode(response.data.thread);
}

export async function fetchProfile(
  actor: string,
  session: SessionFetcher,
): Promise<ProfileView & { indexedAt?: string }> {
  const response = await new Agent(session).getProfile({ actor });
  return response.data;
}

export function recordKey(uri: string | undefined, did: string, collection: string) {
  const parsed = parseAtUri(uri);
  return parsed?.repository === did && parsed.collection === collection
    ? parsed.recordKey
    : undefined;
}

function operationError(nsid: string, error: unknown) {
  const upstreamStatus = error instanceof XRPCError ? error.status : undefined;
  const status =
    upstreamStatus !== undefined && upstreamStatus !== 429 && upstreamStatus < 500 ? 400 : 502;
  const detail = error instanceof Error ? error.message : String(error);
  return new OperationError(`PDS ${nsid} failed: ${detail}`, status);
}

async function writeRecord<T>(nsid: string, request: () => Promise<T>) {
  try {
    return await request();
  } catch (error) {
    throw operationError(nsid, error);
  }
}

export async function putRecord(
  session: SessionFetcher,
  input: {
    repo: string;
    collection: string;
    rkey: string;
    record: Record<string, unknown>;
  },
) {
  const response = await writeRecord("com.atproto.repo.putRecord", () =>
    new Agent(session).com.atproto.repo.putRecord(input),
  );
  return response.data;
}

export async function deleteRecord(
  session: SessionFetcher,
  input: {
    repo: string;
    collection: string;
    rkey: string;
  },
) {
  await writeRecord("com.atproto.repo.deleteRecord", () =>
    new Agent(session).com.atproto.repo.deleteRecord(input),
  );
}
