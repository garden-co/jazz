import type { useDb } from "jazz-tools/react";
import * as Y from "yjs";
import { app } from "../schema.js";
import { HEADER, frame, updates } from "./log.js";

type Db = ReturnType<typeof useDb>;

export function connect(
  db: Db,
  id: string,
  doc: Y.Doc,
  ready: () => void,
  onError: (error: unknown) => void,
) {
  const query = app.documentLogs.where({ documentId: id });
  const origin = Symbol("jazz");
  // Each mounted editor owns a fresh log, including tabs using the same account.
  const logId = crypto.randomUUID();
  const offsets = new Map<string, number>();
  let ownLength = 0;
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

  function applyLog(id: string, contentLog: Uint8Array) {
    if (closed) return;
    const offset = offsets.get(id) ?? 0;
    if (contentLog.length < offset)
      throw new Error("Document log was replaced; reload to reopen it");
    if (contentLog.length === offset) return;
    for (const update of updates(contentLog.subarray(offset), offset === 0)) {
      Y.applyUpdate(doc, update, origin);
    }
    offsets.set(id, contentLog.length);
  }

  function onUpdate(update: Uint8Array, source: unknown) {
    if (source === origin) return;
    const bytes = frame(update);
    enqueue(async () => {
      if (ownLength === 0) {
        const contentLog = new Uint8Array(HEADER.length + bytes.length);
        contentLog.set(HEADER);
        contentLog.set(bytes, HEADER.length);
        await db
          .insert(app.documentLogs, { documentId: id, contentLog }, { id: logId })
          .wait({ tier: "local" });
        ownLength = contentLog.length;
        return;
      }
      await db
        .update(
          app.documentLogs,
          logId,
          {},
          {
            applyDiffs: {
              contentLog: {
                within: { from: ownLength, to: ownLength },
                splices: [{ at: 0, delete: 0, insert: bytes }],
              },
            },
          },
        )
        .wait({ tier: "local" });
      ownLength += bytes.length;
    });
  }

  doc.on("update", onUpdate);
  const unsubscribe = db.subscribe(query.select("contentLog"), {
    onUpdate: (rows) => {
      enqueue(() => {
        if (closed) return;
        for (const row of rows) applyLog(row.id, row.contentLog);
        ready();
      });
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
