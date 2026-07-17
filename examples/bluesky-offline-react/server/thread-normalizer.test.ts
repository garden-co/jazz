import { describe, expect, it } from "vitest";
import { flattenThread } from "./thread-normalizer.js";

describe("flattenThread", () => {
  it("flattens ancestors and replies once with stable parent relationships", () => {
    const root = {
      post: { uri: "at://author/app.bsky.feed.post/root" },
    };
    const selected = {
      post: {
        uri: "at://author/app.bsky.feed.post/selected",
        record: { reply: { root: { uri: root.post.uri }, parent: { uri: root.post.uri } } },
      },
      parent: root,
      replies: [{
        post: { uri: "at://reply/app.bsky.feed.post/child" },
        replies: [{ uri: "at://missing/app.bsky.feed.post/missing", notFound: true }],
      }],
    };

    const result = flattenThread(selected.post.uri, selected);

    expect(result.rootPostId).toBe("at://author/app.bsky.feed.post/root");
    expect(result.selectedPostId).toBe("at://author/app.bsky.feed.post/selected");
    expect(result.entries).toEqual([
      expect.objectContaining({ postId: root.post.uri, parentPostId: undefined, state: "post", sortOrder: 0 }),
      expect.objectContaining({ postId: selected.post.uri, parentPostId: root.post.uri, state: "post", sortOrder: 1 }),
      expect.objectContaining({ postId: "at://reply/app.bsky.feed.post/child", parentPostId: selected.post.uri, state: "post", sortOrder: 2 }),
      expect.objectContaining({ postId: "at://missing/app.bsky.feed.post/missing", parentPostId: "at://reply/app.bsky.feed.post/child", state: "not-found", sortOrder: 3 }),
    ]);
  });

  it("does not duplicate a node repeated by an AppView response", () => {
    const reply = { post: { uri: "at://author/app.bsky.feed.post/reply" } };
    const thread = {
      post: { uri: "at://author/app.bsky.feed.post/root" },
      replies: [reply, reply],
    };

    expect(flattenThread(thread.post.uri, thread).entries).toHaveLength(2);
  });
});
