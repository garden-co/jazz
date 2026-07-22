import { TID } from "@atproto/common-web";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../../schema.js";
import { parseAtRecordUri } from "../../shared/identifiers.js";
import { decodeOperation, operationRow, type Operation } from "../../shared/pending-operations.js";
import { stableObjectId } from "../model/object-id.js";
import { nextReactionIntent } from "../model/reactions.js";
import { writableReplyCount, type DisplayPost } from "../model/timeline-data.js";
import { useOutbox } from "./use-outbox.js";

function recordKey(uri: string | null | undefined, kind: "like" | "repost") {
  const parsed = parseAtRecordUri(uri);
  return parsed?.collection === `app.bsky.feed.${kind}` ? parsed.rkey : undefined;
}

export function useTimelineActions(
  did: string,
  browserOnline: boolean,
  reportApiReachable: (reachable: boolean) => void,
) {
  const db = useDb();
  const jazzSession = useSession();
  const flushOperations = useOutbox(did, browserOnline, reportApiReachable);

  async function publishPost(value: string, replyTo?: { parent: DisplayPost; root: DisplayPost }) {
    if (!value || jazzSession?.user_id !== did) return;
    const parentCid = replyTo?.parent.cid;
    const rootCid = replyTo?.root.cid;
    const reply =
      replyTo && parentCid && rootCid
        ? {
            root: { uri: replyTo.root.uri, cid: rootCid },
            parent: { uri: replyTo.parent.uri, cid: parentCid },
          }
        : undefined;
    if (replyTo && !reply) return;
    const rkey = TID.nextStr();
    const now = new Date().toISOString();
    const uri = `at://${did}/app.bsky.feed.post/${rkey}`;
    const [postId, profileId, entryId, operationId, threadEntryId] = await Promise.all([
      stableObjectId("bluesky-post", uri),
      stableObjectId("bluesky-profile", did),
      stableObjectId("timeline-entry", `${did}:${uri}`),
      stableObjectId("post-operation", `${did}:${uri}`),
      replyTo
        ? stableObjectId("thread-entry", `${replyTo.root.id}:${uri}`)
        : Promise.resolve(undefined),
    ]);

    // The optimistic post and its outbox intention are ordinary Jazz writes.
    // The BFF later applies the intention to ATProto and reconciles these rows.
    db.upsert(
      app.posts,
      {
        uri,
        authorDid: did,
        authorProfileId: profileId,
        text: value,
        createdAt: now,
        indexedAt: now,
        ...(replyTo ? { replyParentId: replyTo.parent.id, replyRootId: replyTo.root.id } : {}),
        replyCount: 0,
        likeCount: 0,
        repostCount: 0,
        state: "pending",
      },
      { id: postId },
    );
    const operation: Operation = {
      id: operationId,
      ownerDid: did,
      kind: "post",
      rkey,
      payload: {
        text: value,
        createdAt: now,
        ...(reply ? { reply } : {}),
      },
      state: "queued",
      createdAt: now,
    };
    db.upsert(app.pendingOperations, operationRow(operation), { id: operationId });
    db.upsert(
      app.timelineEntries,
      {
        ownerDid: did,
        postId,
        threadRootId: replyTo?.root.id ?? postId,
        sortAt: replyTo?.root.indexedAt ?? now,
        active: true,
      },
      { id: entryId },
    );
    if (replyTo && threadEntryId) {
      db.upsert(
        app.threadEntries,
        {
          rootPostId: replyTo.root.id,
          postId,
          parentPostId: replyTo.parent.id,
          sortOrder: 0,
          state: "post",
          indexedAt: now,
        },
        { id: threadEntryId },
      );
      const replyCount = writableReplyCount(replyTo.parent, did);
      if (replyCount !== undefined) db.update(app.posts, replyTo.parent.id, { replyCount });
    }
    flushOperations();
  }

  async function toggleReaction(kind: "like" | "repost", post: DisplayPost) {
    if (!post.cid || jazzSession?.user_id !== did) return;
    const [reactionId, operationId, actorProfileId] = await Promise.all([
      stableObjectId(`bluesky-${kind}`, `${did}:${post.uri}`),
      stableObjectId(`${kind}-operation`, `${did}:${post.uri}`),
      stableObjectId("bluesky-profile", did),
    ]);
    const allOperations = await db.all(app.pendingOperations.where({ ownerDid: { eq: did } }));
    const queued = allOperations.find(
      (operation) => operation.id === operationId && operation.state === "queued",
    );
    const current = kind === "like" ? post.like : post.repost;
    const decodedQueued = queued ? decodeOperation(queued) : undefined;
    const queuedPayload =
      decodedQueued?.kind === "like" || decodedQueued?.kind === "repost"
        ? decodedQueued.payload
        : undefined;
    const intent = nextReactionIntent(current?.active ?? false, queuedPayload);
    const { active, syncedActive } = intent;
    if (queued && !intent.keepPending) {
      if (kind === "like") {
        const row = await db.one(app.likes.where({ id: { eq: reactionId } }));
        if (row) db.update(app.likes, row.id, { active });
      } else {
        const row = await db.one(app.reposts.where({ id: { eq: reactionId } }));
        if (row) db.update(app.reposts, row.id, { active });
      }
      db.delete(app.pendingOperations, queued.id);
      return;
    }
    const rkey =
      queued?.rkey ??
      (active && !syncedActive ? TID.nextStr() : (recordKey(current?.uri, kind) ?? TID.nextStr()));
    const now = new Date().toISOString();
    const uri = current?.uri ?? `at://${did}/app.bsky.feed.${kind}/${rkey}`;
    if (kind === "like") {
      db.upsert(
        app.likes,
        { uri, actorDid: did, subjectPostId: post.id, createdAt: now, active },
        { id: reactionId },
      );
    } else {
      db.upsert(
        app.reposts,
        { uri, actorDid: did, actorProfileId, subjectPostId: post.id, createdAt: now, active },
        { id: reactionId },
      );
    }
    const operation: Operation = {
      id: operationId,
      ownerDid: did,
      kind,
      rkey,
      payload: { subjectUri: post.uri, subjectCid: post.cid, active, syncedActive, createdAt: now },
      state: "queued",
      createdAt: now,
    };
    db.upsert(app.pendingOperations, operationRow(operation), { id: operationId });
    flushOperations();
  }

  return { flushOperations, publishPost, toggleReaction };
}
