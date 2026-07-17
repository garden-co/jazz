import { schema as s } from "jazz-tools";

export default s.defineMigration({
  migrate: {
    "pendingOperations": {
      "operationId": s.drop.string({ backwardsDefault: "" }),
      "target": s.drop.string({ backwardsDefault: "" }),
    },

    "posts": {
      "createdAtMs": s.drop.int({ backwardsDefault: null }),
    },

    "threadEntries": {
    },
  },
  fromHash: "17ab3f062886",
  toHash: "0006b9d637a2",
  from: {
  "pendingOperations": s.table({
    "operationId": s.string(),
    "ownerDid": s.string(),
    "kind": s.string(),
    "target": s.string(),
    "rkey": s.string(),
    "payload": s.string(),
    "state": s.string(),
    "error": s.string().optional(),
    "createdAt": s.string(),
  }),
  "posts": s.table({
    "uri": s.string(),
    "cid": s.string().optional(),
    "authorDid": s.string(),
    "authorProfileId": s.ref("profiles"),
    "text": s.string(),
    "facetsJson": s.string().optional(),
    "createdAt": s.string(),
    "createdAtMs": s.int().optional(),
    "indexedAt": s.string(),
    "replyParentId": s.ref("posts").optional(),
    "replyRootId": s.ref("posts").optional(),
    "replyCount": s.int(),
    "likeCount": s.int(),
    "repostCount": s.int(),
    "state": s.string(),
  }),
  "threadEntries": s.table({
    "rootPostId": s.ref("posts"),
    "postId": s.ref("posts"),
    "parentPostId": s.ref("posts").optional(),
    "sortOrder": s.int(),
    "state": s.string(),
    "indexedAt": s.string(),
  })
},
  to: {
  "pendingOperations": s.table({
    "ownerDid": s.string(),
    "kind": s.enum("like", "post", "repost"),
    "rkey": s.string(),
    "payload": s.string(),
    "state": s.enum("failed", "queued", "sent"),
    "error": s.string().optional(),
    "createdAt": s.string(),
  }),
  "posts": s.table({
    "uri": s.string(),
    "cid": s.string().optional(),
    "authorDid": s.string(),
    "authorProfileId": s.ref("profiles"),
    "text": s.string(),
    "facetsJson": s.string().optional(),
    "createdAt": s.string(),
    "indexedAt": s.string(),
    "replyParentId": s.ref("posts").optional(),
    "replyRootId": s.ref("posts").optional(),
    "replyCount": s.int(),
    "likeCount": s.int(),
    "repostCount": s.int(),
    "state": s.enum("pending", "synced"),
  }),
  "threadEntries": s.table({
    "rootPostId": s.ref("posts"),
    "postId": s.ref("posts"),
    "parentPostId": s.ref("posts").optional(),
    "sortOrder": s.int(),
    "state": s.enum("blocked", "not-found", "post"),
    "indexedAt": s.string(),
  })
},
});
