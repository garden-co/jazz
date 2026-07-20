import type { Operation, PostOperation, ReactionOperation } from "../operations.js";
import {
  deleteRecord,
  fetchViewerPosts,
  OperationError,
  putRecord,
  recordKey,
  type SessionFetcher,
} from "./bluesky.js";
import { createProjectionWriter, type ProjectionWriter } from "./projection-writer.js";
import { normalizePost, stableObjectId } from "./projection-model.js";

type ReconcilerDependencies = {
  deleteRecord: typeof deleteRecord;
  fetchViewerPosts: typeof fetchViewerPosts;
  putRecord: typeof putRecord;
  recordKey: typeof recordKey;
  writer: Pick<ProjectionWriter,
    "completeOperation" | "deactivateRepostTimelineEntries" | "writeLike" | "writePostBundle" | "writeRepost">;
};

export function createReconciler(dependencies: ReconcilerDependencies = {
  deleteRecord,
  fetchViewerPosts,
  putRecord,
  recordKey,
  writer: createProjectionWriter(),
}) {
  async function reconcilePost(did: string, session: SessionFetcher, operation: PostOperation) {
    const record = {
      $type: "app.bsky.feed.post",
      text: operation.payload.text,
      createdAt: operation.payload.createdAt,
      ...(operation.payload.reply ? { reply: operation.payload.reply } : {}),
    };
    const created = await dependencies.putRecord(session, {
      repo: did,
      collection: "app.bsky.feed.post",
      rkey: operation.rkey,
      record,
    });
    const bundle = normalizePost({
      uri: created.uri,
      cid: created.cid,
      author: { did },
      record,
      indexedAt: operation.payload.createdAt,
    });
    if (!bundle) throw new OperationError("PDS returned an invalid post", 502);
    await dependencies.writer.writePostBundle(bundle);
  }

  async function reconcileReaction(
    did: string,
    session: SessionFetcher,
    operation: ReactionOperation,
  ) {
    const kind = operation.kind;
    const collection = `app.bsky.feed.${kind}`;
    // Resolve the current subject again because its CID may have changed while this intention was offline.
    const [post] = await dependencies.fetchViewerPosts(session, [operation.payload.subjectUri]);
    if (!post?.uri || !post.cid) throw new OperationError("subject post is unavailable", 502);

    const postId = stableObjectId("bluesky-post", post.uri);
    const viewerUri = post.viewer?.[kind];
    const wasActive = Boolean(viewerUri);
    let uri = viewerUri;
    let cid: string | undefined;

    if (operation.payload.active && !wasActive) {
      const created = await dependencies.putRecord(session, {
        repo: did,
        collection,
        rkey: operation.rkey,
        record: {
          $type: collection,
          subject: { uri: post.uri, cid: post.cid },
          createdAt: operation.payload.createdAt,
        },
      });
      uri = created.uri;
      cid = created.cid;
    } else if (!operation.payload.active && wasActive) {
      const rkey = dependencies.recordKey(viewerUri, did, collection);
      if (!rkey) throw new OperationError(`AppView returned an invalid ${kind} URI`, 502);
      try {
        await dependencies.deleteRecord(session, { repo: did, collection, rkey });
      } catch (error) {
        if (!(error instanceof OperationError) || !error.message.includes("RecordNotFound")) throw error;
      }
      uri = undefined;
    }

    const bundle = normalizePost({
      ...post,
      likeCount: kind === "like"
        ? Math.max(0, (post.likeCount ?? 0) + Number(operation.payload.active) - Number(wasActive))
        : post.likeCount,
      repostCount: kind === "repost"
        ? Math.max(0, (post.repostCount ?? 0) + Number(operation.payload.active) - Number(wasActive))
        : post.repostCount,
    });
    if (!bundle) throw new OperationError("AppView returned an invalid subject post", 502);
    await dependencies.writer.writePostBundle(bundle);

    const id = stableObjectId(`bluesky-${kind}`, `${did}:${post.uri}`);
    if (kind === "like") {
      await dependencies.writer.writeLike({
        id,
        uri: uri ?? `at://${did}/${collection}/${operation.rkey}`,
        actorDid: did,
        subjectPostId: postId,
        createdAt: operation.payload.createdAt,
        active: operation.payload.active,
      });
    } else {
      await dependencies.writer.writeRepost({
        id,
        uri,
        cid,
        actorDid: did,
        actorProfileId: stableObjectId("bluesky-profile", did),
        subjectPostId: postId,
        createdAt: operation.payload.createdAt,
        active: operation.payload.active,
      });
      if (!operation.payload.active) {
        await dependencies.writer.deactivateRepostTimelineEntries(did, id);
      }
    }
  }

  async function reconcileOperations(did: string, session: SessionFetcher, operations: Operation[]) {
    // ATProto repository writes are ordered intentions; do not parallelise them.
    const ordered = [...operations].sort((left, right) =>
      left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id));
    for (const operation of ordered) {
      if (operation.kind === "post") await reconcilePost(did, session, operation);
      else await reconcileReaction(did, session, operation);
      await dependencies.writer.completeOperation(operation);
    }
  }

  return { reconcileOperations };
}

export type Reconciler = ReturnType<typeof createReconciler>;
