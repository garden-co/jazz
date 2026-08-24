import { app } from "../schema.js";

/**
 * The browser has no file-list subscription until a real folder has been
 * selected. In particular, do not turn the empty initial UI state into an
 * invalid UUID filter.
 */
export function fileListQuery(folderId: string | undefined) {
  if (!folderId) return undefined;
  return app.files
    .where({ folder_id: folderId })
    .select("id", "name", "content_type", "size_bytes")
    .orderBy("name", "asc");
}
