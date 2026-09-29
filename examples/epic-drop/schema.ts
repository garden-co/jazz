import { schema as s } from "jazz-tools";

const role = () => s.enum("viewer", "editor");

const schema = {
  // A display name for an anonymous local-first account, so members and file
  // owners read as people rather than account ids.
  profiles: s.table({
    user_id: s.uuid(),
    name: s.string(),
  }),
  folders: s.table(
    {
      name: s.string(),
      owner_id: s.uuid(),
      // Top-level folders have no parent. Subfolders inherit access from it.
      parent_id: s.uuid().optional(),
    },
    {
      parent: s.rel("folders", "parent_id"),
      filesViaFolder: s.reverse("files", "folder"),
    },
  ),
  // Membership in a shared folder. Access flows down to every subfolder and file.
  folderMembers: s
    .table(
      {
        folder_id: s.uuid(),
        user_id: s.uuid(),
        role: role(),
        // The invite code this member redeemed. Permissions check it against a
        // live invite with the same folder and role at write time.
        invite_code: s.string(),
      },
      { folder: s.rel("folders", "folder_id") },
    )
    .indexOnly(["folder_id", "user_id"]),
  // Invite codes are bearer capabilities. Only the folder owner can read them.
  folderInvites: s.table(
    {
      folder_id: s.uuid(),
      code: s.string(),
      role: role(),
    },
    { folder: s.rel("folders", "folder_id") },
  ),
  files: s
    .table(
      {
        folder_id: s.uuid(),
        name: s.string(),
        content_type: s.string(),
        size_bytes: s.int(),
        owner_id: s.uuid(),
        contents: s.bytes(),
      },
      { folder: s.rel("folders", "folder_id") },
    )
    // The browser always opens one folder at a time.
    .indexOnly(["folder_id"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

/**
 * Folder access is inherited through a bounded policy expansion, so a share
 * reaches this many levels of subfolders. The UI stops offering "New folder"
 * at this depth.
 */
export const MAX_FOLDER_DEPTH = 8;

export type FolderRole = "viewer" | "editor";
