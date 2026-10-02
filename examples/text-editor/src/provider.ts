import type { useDb } from "jazz-tools/react";
import * as Y from "yjs";
import { app } from "../schema.js";
import { frame, updates } from "./log.js";

type Db = ReturnType<typeof useDb>;

export function connect(
  db: Db,
  id: string,
  doc: Y.Doc,
  ready: () => void,
  onError: (error: unknown) => void,
) {
  const query = app.documents.where({ id });
  const origin = Symbol("jazz");
  let offset = 0;
  let closed = false;
  let failed = false;
  let queue = Promise.resolve();

  function fail(error: unknown) {
    failed = true;
    if (!closed) onError(error);
  }

  // Subscription updates and appends share a queue so local writes stay ordered.
  function enqueue(task: () => void | Promise<void>) {
    queue = queue.then(() => (failed ? undefined : task())).catch(fail);
  }

  function applyLog(contentLog: Uint8Array) {
    if (closed) return;
    if (contentLog.length < offset)
      throw new Error("Document log was replaced; reload to reopen it");
    if (contentLog.length === offset) return;
    for (const update of updates(contentLog.subarray(offset), offset === 0)) {
      Y.applyUpdate(doc, update, origin);
    }
    offset = contentLog.length;
    ready();
  }

  function onUpdate(update: Uint8Array, source: unknown) {
    if (source === origin) return;
    const bytes = frame(update);
    enqueue(async () => {
      const row = await db.one(query.select("contentLog"));
      if (!row) throw new Error("Document is unavailable");
      const end = row.contentLog.length;
      await db
        .update(
          app.documents,
          id,
          {},
          {
            applyDiffs: {
              contentLog: {
                within: { from: end, to: end },
                splices: [{ at: 0, delete: 0, insert: bytes }],
              },
            },
          },
        )
        .wait({ tier: "local" });
    });
  }

  doc.on("update", onUpdate);
  const unsubscribe = db.subscribe(query.select("contentLog"), {
    onUpdate: (rows) => {
      if (rows[0]) enqueue(() => applyLog(rows[0].contentLog));
    },
    onError: fail,
  });
  const stopErrors = db.onMutationError(fail);
  return () => {
    closed = true;
    doc.off("update", onUpdate);
    unsubscribe();
    stopErrors();
  };
}
