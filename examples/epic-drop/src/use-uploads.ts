import * as React from "react";
import { useDb } from "jazz-tools/react";
import { UploadCancelled, uploadFile } from "./large-values.js";

export interface UploadTask {
  id: string;
  name: string;
  size: number;
  uploaded: number;
  /** "saving": streamed, being written to this browser's storage. */
  status: "uploading" | "saving" | "failed";
  error?: string;
}

/**
 * Uploads in flight for this tab. An upload leaves the queue once its row is
 * stored durably in this browser (a reload keeps it); a cancelled one leaves
 * because no row appears.
 */
export function useUploads(userId: string | undefined) {
  const db = useDb();
  const [tasks, setTasks] = React.useState<UploadTask[]>([]);
  const controllers = React.useRef(new Map<string, AbortController>());

  const patch = React.useCallback((id: string, change: Partial<UploadTask> | null) => {
    setTasks((current) =>
      change === null
        ? current.filter((task) => task.id !== id)
        : current.map((task) => (task.id === id ? { ...task, ...change } : task)),
    );
  }, []);

  const start = React.useCallback(
    (files: readonly File[], folderId: string) => {
      if (!userId) return;
      for (const file of files) {
        const id = crypto.randomUUID();
        const controller = new AbortController();
        controllers.current.set(id, controller);
        setTasks((current) => [
          ...current,
          { id, name: file.name, size: file.size, uploaded: 0, status: "uploading" },
        ]);
        uploadFile(db, file, {
          folderId,
          ownerId: userId,
          signal: controller.signal,
          onProgress: (uploaded) => patch(id, { uploaded }),
          onSaving: () => patch(id, { status: "saving" }),
        }).then(
          () => patch(id, null),
          (error: unknown) => {
            if (error instanceof UploadCancelled) patch(id, null);
            else patch(id, { status: "failed", error: String((error as Error)?.message ?? error) });
          },
        );
      }
    },
    [db, patch, userId],
  );

  const cancel = React.useCallback((id: string) => {
    controllers.current.get(id)?.abort();
    controllers.current.delete(id);
  }, []);

  const dismiss = React.useCallback((id: string) => patch(id, null), [patch]);

  return { tasks, start, cancel, dismiss };
}
