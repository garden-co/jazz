import { schema as s } from "jazz-tools";

const schema = {
  oauthSessions: s.table({
    sessionKey: s.string(),
    sessionJson: s.string(),
    updatedAt: s.string(),
  }),
  profiles: s.table({
    did: s.string(),
    handle: s.string().optional(),
    displayName: s.string().optional(),
    description: s.string().optional(),
    avatar: s.string().optional(),
    indexedAt: s.string(),
  }),
  posts: s.table({
    uri: s.string(),
    cid: s.string().optional(),
    authorDid: s.string(),
    authorProfileId: s.ref("profiles"),
    text: s.string(),
    facetsJson: s.string().optional(),
    createdAt: s.string(),
    createdAtMs: s.int().optional(),
    indexedAt: s.string(),
    replyParentId: s.ref("posts").optional(),
    replyRootId: s.ref("posts").optional(),
    replyCount: s.int(),
    likeCount: s.int(),
    repostCount: s.int(),
    state: s.string(),
  }),
  postImages: s.table({
    postId: s.ref("posts"),
    postCid: s.string(),
    position: s.int(),
    thumb: s.string(),
    fullsize: s.string(),
    alt: s.string(),
    aspectWidth: s.int().optional(),
    aspectHeight: s.int().optional(),
  }),
  timelineEntries: s.table({
    ownerDid: s.string(),
    postId: s.ref("posts"),
    threadRootId: s.ref("posts"),
    repostId: s.ref("reposts").optional(),
    sortAt: s.string(),
    active: s.boolean(),
  }),
  threadEntries: s.table({
    rootPostId: s.ref("posts"),
    postId: s.ref("posts"),
    parentPostId: s.ref("posts").optional(),
    sortOrder: s.int(),
    state: s.string(),
    indexedAt: s.string(),
  }),
  likes: s.table({
    uri: s.string(),
    actorDid: s.string(),
    subjectPostId: s.ref("posts"),
    createdAt: s.string(),
    active: s.boolean(),
  }),
  reposts: s.table({
    uri: s.string().optional(),
    cid: s.string().optional(),
    actorDid: s.string(),
    actorProfileId: s.ref("profiles"),
    subjectPostId: s.ref("posts"),
    createdAt: s.string(),
    active: s.boolean(),
  }),
  pendingOperations: s.table({
    operationId: s.string(),
    ownerDid: s.string(),
    kind: s.string(),
    target: s.string(),
    rkey: s.string(),
    payload: s.string(),
    state: s.string(),
    error: s.string().optional(),
    createdAt: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Profile = s.RowOf<typeof app.profiles>;
export type Post = s.RowOf<typeof app.posts>;
export type PendingOperation = s.RowOf<typeof app.pendingOperations>;
