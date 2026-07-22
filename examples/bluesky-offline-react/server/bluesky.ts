import { Agent, XRPCError } from "@atproto/api";
import { parseAtRecordUri } from "../shared/identifiers.js";
import type { OAuthSession } from "./auth.js";

export type SessionFetcher = Pick<OAuthSession, "fetchHandler">;

const timelinePageSize = 20;

function retryableReadError(error: unknown) {
  return error instanceof TypeError || (error instanceof XRPCError && error.status >= 500);
}

async function readFromAppView<T>(request: () => Promise<T>) {
  try {
    return await request();
  } catch (error) {
    if (!retryableReadError(error)) throw error;
    return request();
  }
}

export class OperationError extends Error {
  constructor(
    message: string,
    readonly status: 400 | 502,
  ) {
    super(message);
  }
}

export async function fetchTimelineFeed(session: SessionFetcher, cursor?: string) {
  const response = await readFromAppView(() =>
    new Agent(session).getTimeline({
      limit: timelinePageSize,
      ...(cursor ? { cursor } : {}),
    }),
  );
  return response.data;
}

export async function fetchViewerPosts(session: SessionFetcher, uris: string[]) {
  const response = await readFromAppView(() => new Agent(session).getPosts({ uris }));
  return response.data.posts;
}

export async function fetchPostThread(session: SessionFetcher, uri: string) {
  const response = await readFromAppView(() =>
    new Agent(session).getPostThread({ uri, depth: 100, parentHeight: 100 }),
  );
  return response.data.thread;
}

export async function fetchProfile(actor: string, session: SessionFetcher) {
  const response = await readFromAppView(() => new Agent(session).getProfile({ actor }));
  return response.data;
}

export function recordKey(uri: string | undefined, did: string, collection: string) {
  const parsed = parseAtRecordUri(uri);
  return parsed?.repo === did && parsed.collection === collection ? parsed.rkey : undefined;
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
