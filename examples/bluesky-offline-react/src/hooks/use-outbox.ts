import { useDb } from "jazz-tools/react";
import { useEffect, useRef } from "react";
import { app } from "../../schema.js";

const retryInterval = 15_000;

type DeliverableOperation = {
  id: string;
  createdAt: string;
};

type OperationFailure = {
  state: "queued" | "failed";
  error: string;
};

export async function deliverOperations<Operation extends DeliverableOperation>(
  operations: Operation[],
  {
    request = fetch,
    markFailed,
    reportApiReachable,
  }: {
    request?: typeof fetch;
    markFailed: (id: string, failure: OperationFailure) => void;
    reportApiReachable: (reachable: boolean) => void;
  },
) {
  const ordered = [...operations].sort(
    (left, right) =>
      left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
  );

  // Send one intention at a time so a failure identifies the exact operation
  // and later ATProto repository writes remain ordered behind it.
  for (const operation of ordered) {
    try {
      const response = await request("/api/operations", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([operation]),
      });
      const result = (await response.json().catch(() => ({ error: "Sync failed" }))) as {
        error?: string;
      };
      reportApiReachable(true);
      if (!response.ok) {
        markFailed(operation.id, {
          // Authentication may be refreshed without discarding the intention.
          state: response.status === 400 || response.status === 403 ? "failed" : "queued",
          error: result.error ?? "Sync failed",
        });
        return;
      }
    } catch {
      markFailed(operation.id, { state: "queued", error: "Sync failed" });
      reportApiReachable(false);
      return;
    }
  }
}

function singleFlight(task: () => Promise<void>) {
  let inFlight: Promise<void> | undefined;
  return () => {
    if (inFlight) return inFlight;
    const request = task().finally(() => {
      if (inFlight === request) inFlight = undefined;
    });
    inFlight = request;
    return request;
  };
}

export function useOutbox(
  ownerDid: string,
  browserOnline: boolean,
  reportApiReachable: (reachable: boolean) => void,
) {
  const db = useDb();

  async function runFlush() {
    const operations = await db.all(
      app.pendingOperations.where({
        ownerDid: { eq: ownerDid },
        state: { eq: "queued" },
      }),
    );
    if (!operations.length || !navigator.onLine) return;
    await deliverOperations(operations, {
      markFailed: (id, failure) => db.update(app.pendingOperations, id, failure),
      reportApiReachable,
    });
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
