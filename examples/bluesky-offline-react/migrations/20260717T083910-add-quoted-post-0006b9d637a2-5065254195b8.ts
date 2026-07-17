import { schema as s } from "jazz-tools";

export default s.defineMigration({
  migrate: {
    "posts": {
      "quotedPostId": s.add.ref("posts", { default: null }),
    },
  },
  fromHash: "0006b9d637a2",
  toHash: "5065254195b8",
  from: {
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
  })
},
  to: {
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
    "quotedPostId": s.ref("posts").optional(),
    "replyCount": s.int(),
    "likeCount": s.int(),
    "repostCount": s.int(),
    "state": s.enum("pending", "synced"),
  })
},
});
