import { useDb } from "jazz-tools/react";
import { useEffect, useRef } from "react";
import { app } from "../schema.js";
import { stableObjectId } from "./object-id.js";
import { singleFlight } from "./single-flight.js";

const retryInterval = 15_000;

export function useOutbox(
  ownerDid: string,
  browserOnline: boolean,
  reportApiReachable: (reachable: boolean) => void,
) {
  const db = useDb();

  async function runFlush() {
    const operations = await db.all(app.pendingOperations.where({ ownerDid: { eq: ownerDid }, state: { eq: "queued" } }));
    if (!operations.length || !navigator.onLine) return;
    try {
      const response = await fetch("/api/operations", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(operations),
      });
      const result = await response.json().catch(() => ({ error: "Sync failed" })) as { error?: string };
      if (!response.ok) {
        const permanent = response.status === 400 || response.status === 401 || response.status === 403;
        for (const operation of operations) {
          db.update(app.pendingOperations, operation.id, {
            state: permanent ? "failed" : "queued",
            error: result.error ?? "Sync failed",
          });
        }
        reportApiReachable(permanent);
        return;
      }
      reportApiReachable(true);
      for (const operation of operations) {
        db.update(app.pendingOperations, operation.id, { state: "sent", error: "" });
        if (operation.kind === "post") {
          const postId = await stableObjectId("bluesky-post", `at://${ownerDid}/app.bsky.feed.post/${operation.rkey}`);
          const post = await db.one(app.posts.where({ id: { eq: postId } }));
          if (post) db.update(app.posts, post.id, { state: "synced" });
        }
      }
    } catch {
      for (const operation of operations) db.update(app.pendingOperations, operation.id, { error: "Sync failed" });
      reportApiReachable(false);
    }
  }

  const flushState = useRef<{ ownerDid: string; run: () => Promise<void> } | undefined>(undefined);
  if (!flushState.current || flushState.current.ownerDid !== ownerDid) {
    flushState.current = { ownerDid, run: singleFlight(runFlush) };
  }
  const flush = flushState.current.run;

  useEffect(() => {
    if (!browserOnline) return;
    flush();
    const timer = window.setInterval(flush, retryInterval);
    return () => window.clearInterval(timer);
  }, [browserOnline, ownerDid]);

  return flush;
}
