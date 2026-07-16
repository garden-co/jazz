import type { OAuthSession } from "./auth.js";
import type { FeedViewPost, PostView, ProfileView } from "./timeline.js";

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

export async function fetchTimelineFeed(session: OAuthSession, cursor?: string) {
  const params = new URLSearchParams({ limit: String(timelinePageSize) });
  if (cursor) params.set("cursor", cursor);
  const response = await session.fetchHandler(`/xrpc/app.bsky.feed.getTimeline?${params}`, {
    headers: appViewHeaders,
  });
  if (!response.ok) throw new Error(`Timeline request failed (${response.status})`);
  return await response.json() as { cursor?: string; feed?: FeedViewPost[] };
}

export async function fetchViewerPosts(session: OAuthSession, uris: string[]) {
  const params = new URLSearchParams();
  for (const uri of uris) params.append("uris", uri);
  const response = await session.fetchHandler(`/xrpc/app.bsky.feed.getPosts?${params}`, {
    headers: appViewHeaders,
  });
  if (!response.ok) throw new Error(`viewer posts failed: ${response.status}`);
  return (await response.json() as { posts?: PostView[] }).posts ?? [];
}

export async function fetchPostThread(session: OAuthSession, uri: string) {
  const params = new URLSearchParams({ uri, depth: "100", parentHeight: "100" });
  const response = await session.fetchHandler(`/xrpc/app.bsky.feed.getPostThread?${params}`, {
    headers: appViewHeaders,
  });
  if (!response.ok) throw new Error(`Thread request failed (${response.status})`);
  return (await response.json() as { thread?: ThreadViewNode }).thread;
}

export async function fetchProfile(actor: string) {
  const url = new URL("https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile");
  url.searchParams.set("actor", actor);
  const response = await fetch(url);
  if (!response.ok) return undefined;
  return await response.json() as ProfileView & { indexedAt?: string };
}

export function recordKey(uri: string | undefined, did: string, collection: string) {
  const match = uri?.match(/^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/);
  return match?.[1] === did && match[2] === collection ? match[3] : undefined;
}

async function pdsJson(session: OAuthSession, path: string, body: unknown) {
  const response = await session.fetchHandler(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const message = await response.text();
    throw new OperationError(message, response.status === 429 || response.status >= 500 ? 502 : 400);
  }
  const text = await response.text();
  return text ? JSON.parse(text) as { uri: string; cid?: string } : { uri: "" };
}

export function putRecord(session: OAuthSession, input: {
  repo: string;
  collection: string;
  rkey: string;
  record: Record<string, unknown>;
}) {
  return pdsJson(session, "/xrpc/com.atproto.repo.putRecord", input);
}

export async function deleteRecord(session: OAuthSession, input: {
  repo: string;
  collection: string;
  rkey: string;
}) {
  await pdsJson(session, "/xrpc/com.atproto.repo.deleteRecord", input);
}
