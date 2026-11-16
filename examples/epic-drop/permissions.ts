import { schema as s } from "jazz-tools";
import { schema } from "./schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const ownerIdIsMe = { owner_id: session.user };

const userIdIsMe = { user_id: session.user };

const idIsMe = { id: session.user };

export const permissions = definePermissions({
  files: {
    read: (file) => allowedTo.read(file.folder),
    insert: (file) => allOf(ownerIdIsMe, allowedTo.update(file.folder)),
    updateIf: (file) => allowedTo.update(file.folder),
    updateTo: (file, old) =>
      allOf(
        allowedTo.update(file.folder),
        { owner_id: old.owner_id },
        anyOf(
          { folder_id: old.folder_id },
          ownerIdIsMe,
          allOf(
            file.folder.where(ownerIdIsMe),
            exists.files({ id: file.id }).hop("folder", { owner_id: session.user }),
          ),
        ),
      ),
    delete: (file) => allowedTo.update(file.folder),
  },
  folderInvites: {
    read: (folderInvite) => folderInvite.folder.where(ownerIdIsMe),
    insert: (folderInvite) => folderInvite.folder.where(ownerIdIsMe),
    delete: (folderInvite) => folderInvite.folder.where(ownerIdIsMe),
  },
  folderMembers: {
    read: (folderMember) => anyOf(userIdIsMe, folderMember.folder.where(ownerIdIsMe)),
    insert: (folderMember) =>
      allOf(
        userIdIsMe,
        folderMember.folder.where({ owner_id: folderMember.folder_owner_id }),
        exists.folderInvites({
          folder_id: folderMember.folder_id,
          code: folderMember.invite_code,
          role: folderMember.role,
        }),
      ),
    updateIf: (folderMember) => folderMember.folder.where(ownerIdIsMe),
    updateTo: (folderMember) =>
      allOf(
        folderMember.folder.where(ownerIdIsMe),
        exists.folderMembers({
          id: folderMember.id,
          folder_id: folderMember.folder_id,
          user_id: folderMember.user_id,
          folder_owner_id: folderMember.folder_owner_id,
        }),
      ),
    delete: (folderMember) => anyOf(userIdIsMe, folderMember.folder.where(ownerIdIsMe)),
  },
  folders: {
    read: (folder) =>
      anyOf(
        ownerIdIsMe,
        exists.folderMembers({ folder_id: folder.id, user_id: session.user }),
        allowedTo.read(folder.parent, { maxDepth: 8 }),
      ),
    insert: (folder) =>
      allOf(
        anyOf({ parent_id: null }, allowedTo.update(folder.parent, { maxDepth: 8 })),
        anyOf(
          { parent_id: null, owner_id: session.user },
          folder.parent.where({ owner_id: folder.owner_id }),
        ),
      ),
    updateIf: (folder) =>
      anyOf(
        ownerIdIsMe,
        exists.folderMembers({ folder_id: folder.id, user_id: session.user, role: "editor" }),
        allowedTo.update(folder.parent, { maxDepth: 8 }),
      ),
    updateTo: (folder, old) =>
      allOf(
        { owner_id: old.owner_id },
        anyOf(
          { parent_id: old.parent_id },
          allOf({ parent_id: null }, exists.folders({ id: folder.id, parent_id: null })),
          allOf(ownerIdIsMe, anyOf({ parent_id: null }, folder.parent.where(ownerIdIsMe))),
        ),
      ),
    delete: (folder) => anyOf(ownerIdIsMe, allowedTo.update(folder.parent, { maxDepth: 8 })),
  },
  profiles: {
    read: (profile) =>
      anyOf(idIsMe, allowedTo.read(profile.memberships), allowedTo.read(profile.hostedMemberships)),
    insert: idIsMe,
    update: idIsMe,
  },
});
