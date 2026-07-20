import { XRPCError } from "@atproto/api";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  deleteRecord,
  fetchProfile,
  fetchPostThread,
  fetchTimelineFeed,
  fetchViewerPosts,
  putRecord,
  recordKey,
  type SessionFetcher,
} from "./bluesky.js";

const agent = vi.hoisted(() => ({
  constructor: vi.fn(),
  getTimeline: vi.fn(),
  getPosts: vi.fn(),
  getPostThread: vi.fn(),
  getProfile: vi.fn(),
  putRecord: vi.fn(),
  deleteRecord: vi.fn(),
}));

vi.mock("@atproto/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@atproto/api")>();
  return {
    ...actual,
    Agent: class {
      com = { atproto: { repo: { putRecord: agent.putRecord, deleteRecord: agent.deleteRecord } } };
      getTimeline = agent.getTimeline;
      getPosts = agent.getPosts;
      getPostThread = agent.getPostThread;
      getProfile = agent.getProfile;

      constructor(session: SessionFetcher) {
        agent.constructor(session);
      }
    },
  };
});

const session = {
  fetchHandler: vi.fn(async () => {
    throw new Error("the adapter bypassed @atproto/api");
  }),
} satisfies SessionFetcher;

beforeEach(() => {
  vi.clearAllMocks();
});

describe("AppView reads", () => {
  it("loads a timeline page through an authenticated Agent", async () => {
    const page = { feed: [], cursor: "next-page" };
    agent.getTimeline.mockResolvedValue({ data: page });

    await expect(fetchTimelineFeed(session, "current-page")).resolves.toEqual(page);
    expect(agent.constructor).toHaveBeenCalledWith(session);
    expect(agent.getTimeline).toHaveBeenCalledWith({ limit: 20, cursor: "current-page" });
  });

  it("forwards the remaining projection reads to Agent", async () => {
    const uri = "at://did:plc:alice/app.bsky.feed.post/3m12345678921";
    const post = { uri, cid: "bafy-post" };
    const thread = { $type: "app.bsky.feed.defs#threadViewPost", post };
    const profile = { did: "did:plc:alice", handle: "alice.example" };
    agent.getPosts.mockResolvedValue({ data: { posts: [post] } });
    agent.getPostThread.mockResolvedValue({ data: { thread } });
    agent.getProfile.mockResolvedValue({ data: profile });

    await expect(fetchViewerPosts(session, [uri])).resolves.toEqual([post]);
    await expect(fetchPostThread(session, uri)).resolves.toEqual(thread);
    await expect(fetchProfile("did:plc:alice", session)).resolves.toEqual(profile);

    expect(agent.getPosts).toHaveBeenCalledWith({ uris: [uri] });
    expect(agent.getPostThread).toHaveBeenCalledWith({ uri, depth: 100, parentHeight: 100 });
    expect(agent.getProfile).toHaveBeenCalledWith({ actor: "did:plc:alice" });
  });
});

describe("PDS writes", () => {
  const input = {
    repo: "did:plc:alice",
    collection: "app.bsky.feed.post",
    rkey: "3m12345678921",
    record: { text: "hello" },
  };

  it("forwards record writes and deletes to Agent", async () => {
    agent.putRecord.mockResolvedValue({ data: { uri: "at://post", cid: "bafy-post" } });
    agent.deleteRecord.mockResolvedValue({ data: {} });

    await expect(putRecord(session, input)).resolves.toEqual({ uri: "at://post", cid: "bafy-post" });
    await expect(deleteRecord(session, input)).resolves.toBeUndefined();

    expect(agent.putRecord).toHaveBeenCalledWith(input);
    expect(agent.deleteRecord).toHaveBeenCalledWith({
      repo: input.repo,
      collection: input.collection,
      rkey: input.rkey,
    });
  });

  it.each([
    [400, 400],
    [429, 502],
    [503, 502],
  ] as const)("maps an upstream %i response to operation status %i", async (upstream, expected) => {
    agent.putRecord.mockRejectedValue(new XRPCError(upstream, "UpstreamError", "write failed"));

    await expect(putRecord(session, input)).rejects.toMatchObject({ status: expected });
  });

  it("treats transport failures as retryable", async () => {
    agent.putRecord.mockRejectedValue(new TypeError("fetch failed"));

    await expect(putRecord(session, input)).rejects.toMatchObject({ status: 502 });
  });
});

describe("recordKey", () => {
  const did = "did:plc:alice";
  const collection = "app.bsky.feed.like";

  it("returns a key only for the expected repository and collection", () => {
    expect(recordKey(`at://${did}/${collection}/3m12345678921`, did, collection)).toBe("3m12345678921");
    expect(recordKey(`at://did:plc:bob/${collection}/3m12345678921`, did, collection)).toBeUndefined();
    expect(recordKey(`at://${did}/app.bsky.feed.repost/3m12345678921`, did, collection)).toBeUndefined();
    expect(recordKey("not-an-at-uri", did, collection)).toBeUndefined();
  });
});
