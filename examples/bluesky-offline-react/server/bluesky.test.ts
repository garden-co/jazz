import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchProfile,
  fetchPostThread,
  fetchTimelineFeed,
  fetchViewerPosts,
  putRecord,
  type SessionFetcher,
} from "./bluesky.js";

function sessionReturning(response: Response) {
  const fetchHandler = vi.fn(async () => response);
  return { session: { fetchHandler } as SessionFetcher, fetchHandler };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AppView requests", () => {
  it("prefers the authenticated AppView for profile enrichment when a session is available", async () => {
    const { session, fetchHandler } = sessionReturning(Response.json({
      did: "did:plc:alice",
      handle: "alice.example",
    }));

    await expect(fetchProfile("did:plc:alice", session)).resolves.toMatchObject({
      did: "did:plc:alice",
      handle: "alice.example",
    });
    expect(fetchHandler).toHaveBeenCalledWith(
      "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
      { headers: { "atproto-proxy": "did:web:api.bsky.app#bsky_appview" } },
    );
  });

  it("uses explicitly best-effort public profile enrichment without a session", async () => {
    const publicFetch = vi.fn(async () => new Response("unavailable", { status: 503 }));
    vi.stubGlobal("fetch", publicFetch);

    await expect(fetchProfile("did:plc:alice")).resolves.toBeUndefined();
    expect(publicFetch).toHaveBeenCalledWith(
      new URL("https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice"),
      undefined,
    );
  });

  it("reports AppView errors consistently", async () => {
    const timeline = sessionReturning(new Response("upstream unavailable", { status: 503 }));
    const posts = sessionReturning(new Response("upstream unavailable", { status: 503 }));

    await expect(fetchTimelineFeed(timeline.session)).rejects.toThrow(
      "AppView app.bsky.feed.getTimeline failed (503): upstream unavailable",
    );
    await expect(fetchViewerPosts(posts.session, ["at://did:plc:alice/app.bsky.feed.post/1"])).rejects.toThrow(
      "AppView app.bsky.feed.getPosts failed (503): upstream unavailable",
    );
  });

  it("identifies AppView transport failures at the same boundary", async () => {
    const session = {
      fetchHandler: vi.fn(async () => {
        throw new TypeError("fetch failed");
      }),
    } as SessionFetcher;

    await expect(fetchTimelineFeed(session)).rejects.toThrow(
      "AppView app.bsky.feed.getTimeline failed: fetch failed",
    );
  });

  it("rejects malformed protocol responses at the XRPC boundary", async () => {
    const timeline = sessionReturning(Response.json({ feed: "not-an-array" }));
    const posts = sessionReturning(Response.json({ posts: "not-an-array" }));
    const thread = sessionReturning(Response.json({ thread: [] }));
    const profile = sessionReturning(Response.json({ did: 42 }));

    await expect(fetchTimelineFeed(timeline.session)).rejects.toThrow(
      "Invalid app.bsky.feed.getTimeline response",
    );
    await expect(fetchViewerPosts(posts.session, [])).rejects.toThrow(
      "Invalid app.bsky.feed.getPosts response",
    );
    await expect(fetchPostThread(thread.session, "at://did:plc:alice/app.bsky.feed.post/1")).rejects.toThrow(
      "Invalid app.bsky.feed.getPostThread response",
    );
    await expect(fetchProfile("did:plc:alice", profile.session)).rejects.toThrow(
      "Invalid app.bsky.actor.getProfile response",
    );
  });
});

describe("PDS requests", () => {
  it("maps retryable XRPC failures to a gateway operation error", async () => {
    const { session } = sessionReturning(new Response("rate limited", { status: 429 }));

    const result = putRecord(session, {
      repo: "did:plc:alice",
      collection: "app.bsky.feed.post",
      rkey: "3m12345678921",
      record: { text: "hello" },
    });

    await expect(result).rejects.toMatchObject({
      message: "PDS com.atproto.repo.putRecord failed (429): rate limited",
      status: 502,
    });
  });
});
