import type { ThreadViewNode } from "./bluesky.js";

export type FlatThreadEntry = {
  node: ThreadViewNode;
  postId: string;
  parentPostId?: string;
  sortOrder: number;
  state: "post" | "blocked" | "not-found";
};

export type FlatThread = {
  rootPostId: string;
  selectedPostId: string;
  entries: FlatThreadEntry[];
};

export function flattenThread(
  requestedUri: string,
  thread: ThreadViewNode,
  toPostId: (uri: string) => string = (uri) => uri,
): FlatThread {
  const ancestors: ThreadViewNode[] = [];
  for (let node: ThreadViewNode | undefined = thread; node; node = node.parent) {
    ancestors.unshift(node);
  }

  const selectedUri = thread.post?.uri ?? thread.uri ?? requestedUri;
  const rootUri = thread.post?.record?.reply?.root?.uri
    ?? ancestors[0]?.post?.uri
    ?? ancestors[0]?.uri
    ?? requestedUri;
  const entries: FlatThreadEntry[] = [];
  const seen = new Set<string>();

  const addNode = (node: ThreadViewNode, fallbackParentId?: string) => {
    const uri = node.post?.uri ?? node.uri;
    if (!uri) return undefined;
    const postId = toPostId(uri);
    if (seen.has(postId)) return postId;
    seen.add(postId);
    const parentUri = node.post?.record?.reply?.parent?.uri;
    entries.push({
      node,
      postId,
      parentPostId: parentUri ? toPostId(parentUri) : fallbackParentId,
      sortOrder: entries.length,
      state: node.post
        ? "post"
        : node.blocked || node.$type?.endsWith("#blockedPost")
          ? "blocked"
          : "not-found",
    });
    return postId;
  };

  let parentId: string | undefined;
  for (const ancestor of ancestors) {
    parentId = addNode(ancestor, parentId) ?? parentId;
  }

  const addReplies = (nodes: ThreadViewNode[] | undefined, replyParentId: string) => {
    for (const node of nodes ?? []) {
      const postId = addNode(node, replyParentId);
      if (postId) addReplies(node.replies, postId);
    }
  };
  addReplies(thread.replies, toPostId(selectedUri));

  return {
    rootPostId: toPostId(rootUri),
    selectedPostId: toPostId(selectedUri),
    entries,
  };
}
