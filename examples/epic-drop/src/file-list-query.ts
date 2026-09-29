import { app } from "../schema.js";

/**
 * A folder listing is metadata-only. In particular, a list must not select the
 * potentially indirect `contents` cell just to render filename and size.
 */
export function fileListQuery(folderId: string | undefined) {
  if (!folderId) return undefined;
  return app.files
    .where({ folder_id: folderId })
    .select("id", "name", "content_type", "size_bytes")
    .orderBy("name", "asc");
}

/** The file table's columns: the listing above plus owner and last change. */
export function fileTableQuery(folderId: string | undefined) {
  if (!folderId) return undefined;
  return app.files
    .where({ folder_id: folderId })
    .select("id", "name", "content_type", "size_bytes", "owner_id", "folder_id", "$updatedAt")
    .orderBy("name", "asc");
}
