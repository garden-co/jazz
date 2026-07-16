import { schema as s } from "jazz-tools";

export default s.defineMigration({
  migrate: {
    "posts": {
      "facetsJson": s.add.string({ default: null }),
    },
  },
  fromHash: "ee8f1a2393d5",
  toHash: "17ab3f062886",
  from: {
  "posts": s.table({
    "uri": s.string(),
    "cid": s.string().optional(),
    "authorDid": s.string(),
    "authorProfileId": s.ref("profiles"),
    "text": s.string(),
    "createdAt": s.string(),
    "createdAtMs": s.int().optional(),
    "indexedAt": s.string(),
    "replyParentId": s.ref("posts").optional(),
    "replyRootId": s.ref("posts").optional(),
    "replyCount": s.int(),
    "likeCount": s.int(),
    "repostCount": s.int(),
    "state": s.string(),
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
    "createdAtMs": s.int().optional(),
    "indexedAt": s.string(),
    "replyParentId": s.ref("posts").optional(),
    "replyRootId": s.ref("posts").optional(),
    "replyCount": s.int(),
    "likeCount": s.int(),
    "repostCount": s.int(),
    "state": s.string(),
  })
},
});
