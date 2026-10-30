import { app } from "../schema.js";

/**
 * The file table's columns. A folder listing is metadata-only: it must not
 * select the potentially large `contents` value just to render a row.
 */
export function fileTableQuery(folderId: string | undefined) {
  if (!folderId) return undefined;
  return app.files
    .where({ folder_id: folderId })
    .select("id", "name", "content_type", "size_bytes", "owner_id", "folder_id", "$updatedAt")
    .orderBy("name", "asc");
}
