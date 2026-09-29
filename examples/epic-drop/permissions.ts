import { schema as s } from "jazz-tools";
import { app, MAX_FOLDER_DEPTH } from "./schema.js";

/**
 * Folder access is inherited down the tree. A folder is visible to its owner,
 * to members of that folder, and to anyone who can see its parent. Editing its
 * contents follows the same shape with owners, editor members and editable
 * parents. Moving or deleting a folder is reserved for its owner and for
 * people who can edit its parent, so an invite never lets someone take the
 * shared folder itself somewhere else.
 */

export default s.definePermissions(app, ({ allOf, anyOf, allowedTo, policy, session }) => {
  const me = session.user.account;
  const depth = { maxDepth: MAX_FOLDER_DEPTH };

  // A profile's id is its account id. A name is visible to its account and
  // across a membership: owners see their members, members see the owner.
  policy.profiles.allowRead.where(
    anyOf([{ id: me }, allowedTo.read("memberships"), allowedTo.read("hostedMemberships")]),
  );
  policy.profiles.allowInsert.where({ id: me });
  policy.profiles.allowUpdate.where({ id: me });

  // A top-level folder, or one inside a folder the caller can edit.
  const placementAllowed = anyOf([
    { parent_id: { isNull: true } },
    allowedTo.update("parent", depth),
  ]);

  policy.folders.allowRead.where((folder) =>
    anyOf([
      { owner_id: me },
      policy.folderMembers.exists.where({ folder_id: folder.id, user_id: me }),
      allowedTo.read("parent", depth),
    ]),
  );
  policy.folders.allowInsert.where(allOf([{ owner_id: me }, placementAllowed]));
  policy.folders.allowUpdate
    .whereOld((folder) =>
      anyOf([
        { owner_id: me },
        policy.folderMembers.exists.where({ folder_id: folder.id, user_id: me, role: "editor" }),
        allowedTo.update("parent", depth),
      ]),
    )
    .whereNew((folder) =>
      allOf([
        // Renames and moves never transfer ownership, which carries sharing rights.
        policy.folders.exists.where({ id: folder.id, owner_id: folder.owner_id }),
        anyOf([
          // Editors rename a folder in place...
          policy.folders.exists.where({ id: folder.id, parent_id: folder.parent_id }),
          allOf([
            { parent_id: { isNull: true } },
            policy.folders.exists.where({ id: folder.id, parent_id: { isNull: true } }),
          ]),
          // ...only its owner moves it, and only into a folder they can edit.
          allOf([{ owner_id: me }, placementAllowed]),
        ]),
      ]),
    );
  policy.folders.allowDelete.where(anyOf([{ owner_id: me }, allowedTo.update("parent", depth)]));

  // Membership rows keep the invite code they were created with, so only the
  // member and the folder owner read them: a viewer must not learn an editor's code.
  policy.folderMembers.allowRead.where((member) =>
    anyOf([{ user_id: me }, policy.folders.exists.where({ id: member.folder_id, owner_id: me })]),
  );
  // Joining requires a live invite for the same folder, code and role. The
  // check runs at the sync server, which sees invites the caller cannot read.
  policy.folderMembers.allowInsert.where((member) =>
    allOf([
      { user_id: me },
      policy.folders.exists.where({ id: member.folder_id, owner_id: member.folder_owner_id }),
      policy.folderInvites.exists.where({
        folder_id: member.folder_id,
        code: member.invite_code,
        role: member.role,
      }),
    ]),
  );
  // Only the folder owner changes a member's role.
  policy.folderMembers.allowUpdate
    .whereOld((member) => policy.folders.exists.where({ id: member.folder_id, owner_id: me }))
    .whereNew((member) =>
      allOf([
        policy.folders.exists.where({ id: member.folder_id, owner_id: me }),
        policy.folderMembers.exists.where({
          id: member.id,
          folder_id: member.folder_id,
          user_id: member.user_id,
          folder_owner_id: member.folder_owner_id,
        }),
      ]),
    );
  // Members leave; owners remove.
  policy.folderMembers.allowDelete.where((member) =>
    anyOf([{ user_id: me }, policy.folders.exists.where({ id: member.folder_id, owner_id: me })]),
  );

  // Invite codes are bearer capabilities, visible only to the folder owner.
  policy.folderInvites.allowRead.where((invite) =>
    policy.folders.exists.where({ id: invite.folder_id, owner_id: me }),
  );
  policy.folderInvites.allowInsert.where((invite) =>
    policy.folders.exists.where({ id: invite.folder_id, owner_id: me }),
  );
  policy.folderInvites.allowDelete.where((invite) =>
    policy.folders.exists.where({ id: invite.folder_id, owner_id: me }),
  );
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
