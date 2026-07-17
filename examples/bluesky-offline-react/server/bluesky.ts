import type { OAuthSession } from "./auth.js";
import { parseAtUri } from "../at-uri.js";
import type {
  FeedViewPost,
  PostImageView,
  PostRecord,
  PostView,
  ProfileView,
  RepostReason,
  StrongRef,
} from "./timeline.js";

type DirectSessionFetcher = Pick<OAuthSession, "fetchHandler">;

export type SessionFetcher = DirectSessionFetcher | {
  did: string;
  session: DirectSessionFetcher;
};

const timelinePageSize = 20;
const appViewHeaders = {
  "atproto-proxy": "did:web:api.bsky.app#bsky_appview",
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

export class OperationError extends Error {
  constructor(message: string, readonly status: 400 | 502) {
    super(message);
  }
}

type Decoder<T> = (value: unknown) => value is T;
type XrpcSource = "AppView" | "PDS" | "Public AppView";

// This example does not depend on @atproto/api, and @atproto/oauth-client-node does not export
// generated AppView response types. These guards therefore validate only the protocol fields
// consumed by the Jazz projection while allowing unrelated lexicon fields to pass through.
function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isOptionalString(value: unknown) {
  return value === undefined || typeof value === "string";
}

function isOptionalNumber(value: unknown) {
  return value === undefined || typeof value === "number";
}

function isOptionalBoolean(value: unknown) {
  return value === undefined || typeof value === "boolean";
}

function isProfileView(value: unknown): value is ProfileView {
  return isObject(value)
    && isOptionalString(value.did)
    && isOptionalString(value.handle)
    && isOptionalString(value.displayName)
    && isOptionalString(value.description)
    && isOptionalString(value.avatar);
}

function isStrongRef(value: unknown): value is StrongRef {
  return isObject(value) && isOptionalString(value.uri) && isOptionalString(value.cid);
}

function isPostRecord(value: unknown): value is PostRecord {
  if (!isObject(value)
    || !isOptionalString(value.text)
    || !isOptionalString(value.createdAt)) return false;
  if (value.reply !== undefined && (!isObject(value.reply)
    || (value.reply.parent !== undefined && !isStrongRef(value.reply.parent))
    || (value.reply.root !== undefined && !isStrongRef(value.reply.root)))) return false;
  if (value.facets === undefined) return true;
  return Array.isArray(value.facets) && value.facets.every((facet) => isObject(facet)
    && (facet.index === undefined || (isObject(facet.index)
      && isOptionalNumber(facet.index.byteStart)
      && isOptionalNumber(facet.index.byteEnd)))
    && (facet.features === undefined || (Array.isArray(facet.features)
      && facet.features.every((feature) => isObject(feature)
        && isOptionalString(feature.$type)
        && isOptionalString(feature.uri)))));
}

function isPostImageView(value: unknown): value is PostImageView {
  return isObject(value)
    && isOptionalString(value.thumb)
    && isOptionalString(value.fullsize)
    && isOptionalString(value.alt)
    && (value.aspectRatio === undefined || (isObject(value.aspectRatio)
      && isOptionalNumber(value.aspectRatio.width)
      && isOptionalNumber(value.aspectRatio.height)));
}

function isPostView(value: unknown): value is PostView {
  return isObject(value)
    && isOptionalString(value.uri)
    && isOptionalString(value.cid)
    && (value.author === undefined || isProfileView(value.author))
    && (value.record === undefined || isPostRecord(value.record))
    && isOptionalString(value.indexedAt)
    && isOptionalNumber(value.replyCount)
    && isOptionalNumber(value.likeCount)
    && isOptionalNumber(value.repostCount)
    && (value.viewer === undefined || (isObject(value.viewer)
      && isOptionalString(value.viewer.like)
      && isOptionalString(value.viewer.repost)))
    && (value.embed === undefined || (isObject(value.embed)
      && (value.embed.images === undefined || (Array.isArray(value.embed.images)
        && value.embed.images.every(isPostImageView)))
      && (value.embed.media === undefined || (isObject(value.embed.media)
        && (value.embed.media.images === undefined || (Array.isArray(value.embed.media.images)
          && value.embed.media.images.every(isPostImageView)))))));
}

function isRepostReason(value: unknown): value is RepostReason {
  return isObject(value)
    && isOptionalString(value.$type)
    && (value.by === undefined || isProfileView(value.by))
    && isOptionalString(value.uri)
    && isOptionalString(value.cid)
    && isOptionalString(value.indexedAt);
}

function isFeedViewPost(value: unknown): value is FeedViewPost {
  return isObject(value)
    && (value.post === undefined || isPostView(value.post))
    && (value.reply === undefined || (isObject(value.reply)
      && (value.reply.root === undefined || isPostView(value.reply.root))
      && (value.reply.parent === undefined || isPostView(value.reply.parent))))
    && (value.reason === undefined || isRepostReason(value.reason));
}

function isThreadViewNode(value: unknown): value is ThreadViewNode {
  return isObject(value)
    && isOptionalString(value.$type)
    && isOptionalString(value.uri)
    && isOptionalBoolean(value.blocked)
    && isOptionalBoolean(value.notFound)
    && (value.post === undefined || isPostView(value.post))
    && (value.parent === undefined || isThreadViewNode(value.parent))
    && (value.replies === undefined || (Array.isArray(value.replies)
      && value.replies.every(isThreadViewNode)));
}

function isTimelineResponse(value: unknown): value is { cursor?: string; feed?: FeedViewPost[] } {
  return isObject(value)
    && isOptionalString(value.cursor)
    && (value.feed === undefined || (Array.isArray(value.feed) && value.feed.every(isFeedViewPost)));
}

function isPostsResponse(value: unknown): value is { posts?: PostView[] } {
  return isObject(value)
    && (value.posts === undefined || (Array.isArray(value.posts) && value.posts.every(isPostView)));
}

function isThreadResponse(value: unknown): value is { thread?: ThreadViewNode } {
  return isObject(value) && (value.thread === undefined || isThreadViewNode(value.thread));
}

function isProfileResponse(value: unknown): value is ProfileView & { indexedAt?: string } {
  return isObject(value) && isOptionalString(value.indexedAt) && isProfileView(value);
}

function isPutRecordResponse(value: unknown): value is { uri: string; cid?: string } {
  return isObject(value) && typeof value.uri === "string" && isOptionalString(value.cid);
}

async function xrpcJson<T>(
  source: XrpcSource,
  nsid: string,
  request: () => Promise<Response>,
  decode: Decoder<T>,
): Promise<T> {
  let response: Response;
  try {
    response = await request();
  } catch (error) {
    throw xrpcError(source, nsid, error instanceof Error ? error.message : String(error));
  }
  const body = await response.text();
  if (!response.ok) {
    throw xrpcError(source, nsid, body, response.status);
  }

  let value: unknown;
  try {
    value = body ? JSON.parse(body) : undefined;
  } catch (error) {
    throw new Error(`Invalid ${nsid} response`, { cause: error });
  }
  if (!decode(value)) throw new Error(`Invalid ${nsid} response`);
  return value;
}

function xrpcError(source: XrpcSource, nsid: string, detail: string, status?: number) {
  const message = `${source} ${nsid} failed${status === undefined ? "" : ` (${status})`}${detail ? `: ${detail}` : ""}`;
  return source === "PDS"
    ? new OperationError(message, status === undefined || status === 429 || status >= 500 ? 502 : 400)
    : new Error(message);
}

function appViewJson<T>(
  session: SessionFetcher,
  nsid: string,
  params: URLSearchParams,
  decode: Decoder<T>,
) {
  const query = params.size ? `?${params}` : "";
  return xrpcJson("AppView", nsid, () => sessionFetch(session, `/xrpc/${nsid}${query}`, {
    headers: appViewHeaders,
  }), decode);
}

function sessionFetch(session: SessionFetcher, input: string, init?: RequestInit) {
  return "fetchHandler" in session
    ? session.fetchHandler(input, init)
    : session.session.fetchHandler(input, init);
}

async function publicAppViewJson<T>(nsid: string, params: URLSearchParams, decode: Decoder<T>) {
  const url = new URL(`/xrpc/${nsid}`, "https://public.api.bsky.app");
  url.search = params.toString();
  try {
    return await xrpcJson("Public AppView", nsid, () => fetch(url, undefined), decode);
  } catch {
    return undefined;
  }
}

export function fetchTimelineFeed(session: SessionFetcher, cursor?: string) {
  const params = new URLSearchParams({ limit: String(timelinePageSize) });
  if (cursor) params.set("cursor", cursor);
  return appViewJson(session, "app.bsky.feed.getTimeline", params, isTimelineResponse);
}

export async function fetchViewerPosts(session: SessionFetcher, uris: string[]) {
  const params = new URLSearchParams();
  for (const uri of uris) params.append("uris", uri);
  const response = await appViewJson(session, "app.bsky.feed.getPosts", params, isPostsResponse);
  return response.posts ?? [];
}

export async function fetchPostThread(session: SessionFetcher, uri: string) {
  const params = new URLSearchParams({ uri, depth: "100", parentHeight: "100" });
  const response = await appViewJson(session, "app.bsky.feed.getPostThread", params, isThreadResponse);
  return response.thread;
}

/**
 * Profile enrichment is authenticated when a session is supplied. Existing callers without a
 * session deliberately use the public AppView as best-effort enrichment and receive `undefined`
 * for transport, HTTP, or response-shape failures.
 */
export function fetchProfile(actor: string, session: SessionFetcher): Promise<ProfileView & { indexedAt?: string }>;
export function fetchProfile(actor: string): Promise<(ProfileView & { indexedAt?: string }) | undefined>;
export function fetchProfile(actor: string, session?: SessionFetcher) {
  const params = new URLSearchParams({ actor });
  return session
    ? appViewJson(session, "app.bsky.actor.getProfile", params, isProfileResponse)
    : publicAppViewJson("app.bsky.actor.getProfile", params, isProfileResponse);
}

export function recordKey(uri: string | undefined, did: string, collection: string) {
  const parsed = parseAtUri(uri);
  return parsed?.repository === did && parsed.collection === collection
    ? parsed.recordKey
    : undefined;
}

function pdsJson<T>(
  session: SessionFetcher,
  nsid: string,
  body: unknown,
  decode: Decoder<T>,
) {
  return xrpcJson("PDS", nsid, () => sessionFetch(session, `/xrpc/${nsid}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  }), decode);
}

export function putRecord(session: SessionFetcher, input: {
  repo: string;
  collection: string;
  rkey: string;
  record: Record<string, unknown>;
}) {
  return pdsJson(session, "com.atproto.repo.putRecord", input, isPutRecordResponse);
}

export async function deleteRecord(session: SessionFetcher, input: {
  repo: string;
  collection: string;
  rkey: string;
}) {
  await pdsJson(session, "com.atproto.repo.deleteRecord", input, (value): value is undefined => value === undefined);
}
