import { schema as s } from "jazz-tools";
import { MAX_FOLDER_DEPTH } from "./app-extras";
import { schema } from "./schema";

/**
 * Folder access is inherited down the tree. A folder is visible to its owner,
 * to members of that folder, and to anyone who can see its parent. Editing its
 * contents follows the same shape with owners, editor members and editable
 * parents.
 *
 * Every folder in a tree belongs to the owner of its top-level folder: a
 * subfolder takes its parent's owner, even when an editor creates it, and a
 * folder only moves by its owner and only into another folder they own. So
 * nothing an invite reaches can be carried out of the owner's tree, which
 * would re-share it with whoever can see the new place. Files follow the same
 * idea: a file changes folder only by its uploader or the owner of the tree.
 */
const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const depth = { maxDepth: MAX_FOLDER_DEPTH };

const ownedByMe = { owner_id: session.user };

const mine = { user_id: session.user };

// A profile's id is its account id.
const myProfile = { id: session.user };

const iOwnFolder = (folderId: s.Uuid) => exists.folders({ id: folderId, owner_id: session.user });

const isFolderMember = (folderId: s.Uuid) =>
  exists.folderMembers({ folder_id: folderId, user_id: session.user });

const isFolderEditor = (folderId: s.Uuid) =>
  exists.folderMembers({ folder_id: folderId, user_id: session.user, role: "editor" });

export const permissions = definePermissions({
  // Files follow their folder. Uploads are stamped with the uploader. Editors
  // rename files in place; a file changes folder only by its uploader, or by
  // the owner of the tree when both folders are theirs.
  files: {
    read: (file) => allowedTo.read(file.folder),
    insert: (file) => allOf(ownedByMe, allowedTo.update(file.folder)),
    updateIf: (file) => allowedTo.update(file.folder),
    updateTo: (file, old) =>
      allOf(
        allowedTo.update(file.folder),
        { owner_id: old.owner_id },
        anyOf(
          { folder_id: old.folder_id },
          ownedByMe,
          allOf(
            iOwnFolder(file.folder_id),
            exists.files({ id: file.id }).hop("folder", { owner_id: session.user }),
          ),
        ),
      ),
    delete: (file) => allowedTo.update(file.folder),
  },
  // Invite codes are bearer capabilities, visible only to the folder owner.
  folderInvites: {
    read: (invite) => iOwnFolder(invite.folder_id),
    insert: (invite) => iOwnFolder(invite.folder_id),
    delete: (invite) => iOwnFolder(invite.folder_id),
  },
  folderMembers: {
    // Membership rows keep the invite code they were created with, so only the
    // member and the folder owner read them: a viewer must not learn an
    // editor's code.
    read: (member) => anyOf(mine, iOwnFolder(member.folder_id)),
    // Joining requires a live invite for the same folder, code and role. The
    // check runs at the sync server, which sees invites the caller cannot read.
    insert: (member) =>
      allOf(
        mine,
        member.folder.where({ owner_id: member.folder_owner_id }),
        exists.folderInvites({
          folder_id: member.folder_id,
          code: member.invite_code,
          role: member.role,
        }),
      ),
    // Only the folder owner changes a member's role.
    updateIf: (member) => iOwnFolder(member.folder_id),
    updateTo: (member) =>
      allOf(
        iOwnFolder(member.folder_id),
        exists.folderMembers({
          id: member.id,
          folder_id: member.folder_id,
          user_id: member.user_id,
          folder_owner_id: member.folder_owner_id,
        }),
      ),
    // Members leave; owners remove.
    delete: (member) => anyOf(mine, iOwnFolder(member.folder_id)),
  },
  folders: {
    read: (folder) =>
      anyOf(ownedByMe, isFolderMember(folder.id), allowedTo.read(folder.parent, depth)),
    insert: (folder) =>
      allOf(
        // A top-level folder, or one inside a folder the caller can edit.
        anyOf({ parent_id: null }, allowedTo.update(folder.parent, depth)),
        // A subfolder takes its parent's owner.
        anyOf(
          { parent_id: null, owner_id: session.user },
          folder.parent.where({ owner_id: folder.owner_id }),
        ),
      ),
    updateIf: (folder) =>
      anyOf(ownedByMe, isFolderEditor(folder.id), allowedTo.update(folder.parent, depth)),
    updateTo: (folder, old) =>
      allOf(
        // Renames and moves never transfer ownership, which carries sharing rights.
        { owner_id: old.owner_id },
        anyOf(
          // Editors rename a folder in place...
          { parent_id: old.parent_id },
          allOf({ parent_id: null }, exists.folders({ id: folder.id, parent_id: null })),
          // ...only its owner moves it, and only within what they own.
          allOf(ownedByMe, anyOf({ parent_id: null }, iOwnFolder(folder.parent_id))),
        ),
      ),
    delete: (folder) => anyOf(ownedByMe, allowedTo.update(folder.parent, depth)),
  },
  profiles: {
    // A name is visible to its account and across a membership: owners see
    // their members, members see the owner.
    read: (profile) =>
      anyOf(
        myProfile,
        allowedTo.read(profile.memberships),
        allowedTo.read(profile.hostedMemberships),
      ),
    insert: myProfile,
    update: myProfile,
  },
});
