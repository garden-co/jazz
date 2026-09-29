import { schema as s } from "jazz-tools";
import { app, MAX_FOLDER_DEPTH } from "./schema.js";

/**
 * Folder access is inherited down the tree. A folder is visible to its owner,
 * to members of that folder, and to anyone who can see its parent. Editing
 * follows the same shape with owners, editor members and editable parents.
 */

export default s.definePermissions(app, ({ allOf, anyOf, allowedTo, policy, session }) => {
  const me = session.user.account;
  const depth = { maxDepth: MAX_FOLDER_DEPTH };

  const isMember = (folderId: unknown) =>
    policy.folderMembers.exists.where({ folder_id: folderId as never, user_id: me });
  const isEditor = (folderId: unknown) =>
    policy.folderMembers.exists.where({
      folder_id: folderId as never,
      user_id: me,
      role: "editor",
    });
  const ownsFolder = (folderId: unknown) =>
    policy.folders.exists.where({ id: folderId as never, owner_id: me });

  // Names are public so shared folders can show who owns and edits what.
  policy.profiles.allowRead.where({});
  policy.profiles.allowInsert.where({ user_id: me });
  policy.profiles.allowUpdate.whereOld({ user_id: me }).whereNew({ user_id: me });

  // A top-level folder, or one inside a folder the caller can edit.
  const placementAllowed = anyOf([
    { parent_id: { isNull: true } },
    allowedTo.update("parent", depth),
  ]);

  policy.folders.allowRead.where((folder) =>
    anyOf([{ owner_id: me }, isMember(folder.id), allowedTo.read("parent", depth)]),
  );
  policy.folders.allowInsert.where(allOf([{ owner_id: me }, placementAllowed]));
  const canEditFolder = (folder: { id: unknown }) =>
    anyOf([{ owner_id: me }, isEditor(folder.id), allowedTo.update("parent", depth)]);
  policy.folders.allowUpdate.whereOld(canEditFolder).whereNew((folder) =>
    allOf([
      placementAllowed,
      // Renames and moves never transfer ownership, which carries sharing rights.
      policy.folders.exists.where({ id: folder.id, owner_id: folder.owner_id }),
    ]),
  );
  policy.folders.allowDelete.where(canEditFolder);

  // Members see who else shares the folder. Only the folder owner manages them.
  policy.folderMembers.allowRead.where((member) =>
    anyOf([{ user_id: me }, ownsFolder(member.folder_id), isMember(member.folder_id)]),
  );
  // Joining requires a live invite for the same folder, code and role. The
  // check runs at the sync server, which sees invites the caller cannot read.
  policy.folderMembers.allowInsert.where((member) =>
    allOf([
      { user_id: me },
      policy.folderInvites.exists.where({
        folder_id: member.folder_id,
        code: member.invite_code,
        role: member.role,
      }),
    ]),
  );
  policy.folderMembers.allowUpdate
    .whereOld((member) => ownsFolder(member.folder_id))
    .whereNew((member) =>
      allOf([
        ownsFolder(member.folder_id),
        policy.folderMembers.exists.where({
          id: member.id,
          folder_id: member.folder_id,
          user_id: member.user_id,
        }),
      ]),
    );
  policy.folderMembers.allowDelete.where((member) =>
    anyOf([{ user_id: me }, ownsFolder(member.folder_id)]),
  );

  policy.folderInvites.allowRead.where((invite) => ownsFolder(invite.folder_id));
  policy.folderInvites.allowInsert.where((invite) => ownsFolder(invite.folder_id));
  policy.folderInvites.allowDelete.where((invite) => ownsFolder(invite.folder_id));

  // Files follow their folder. Uploads are stamped with the uploader, and a
  // file can only be moved between folders the caller can edit.
  policy.files.allowRead.where(allowedTo.read("folder"));
  policy.files.allowInsert.where(allOf([{ owner_id: me }, allowedTo.update("folder")]));
  policy.files.allowUpdate
    .whereOld(allowedTo.update("folder"))
    .whereNew((file) =>
      allOf([
        allowedTo.update("folder"),
        policy.files.exists.where({ id: file.id, owner_id: file.owner_id }),
      ]),
    );
  policy.files.allowDelete.where(allowedTo.update("folder"));
});
