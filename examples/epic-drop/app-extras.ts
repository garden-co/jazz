// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema itself is jazz.app.json; its types are in jazz.generated.ts.

/**
 * Folder access is inherited through a bounded policy expansion, so a share
 * reaches this many levels of subfolders. The UI stops offering "New folder"
 * at this depth.
 */
export const MAX_FOLDER_DEPTH = 8;
export type FolderRole = "viewer" | "editor";
