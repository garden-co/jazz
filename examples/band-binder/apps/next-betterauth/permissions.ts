import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, anyOf }) => {
  type WorkspaceScoped = { workspaceId: string };
  const membership = (row: RowContext<WorkspaceScoped>) =>
    policy.members.exists.where({ workspaceId: row.workspaceId, subject: session.user_id });
  const management = (row: RowContext<WorkspaceScoped>) =>
    anyOf([
      policy.members.exists.where({
        workspaceId: row.workspaceId,
        subject: session.user_id,
        role: "owner",
      }),
      policy.members.exists.where({
        workspaceId: row.workspaceId,
        subject: session.user_id,
        role: "stage_manager",
      }),
    ]);

  policy.workspaces.allowRead.where((workspace) =>
    anyOf([
      { ownerSubject: session.user_id },
      policy.members.exists.where({ workspaceId: workspace.id, subject: session.user_id }),
    ]),
  );
  policy.workspaces.allowInsert.where({ ownerSubject: session.user_id });
  policy.workspaces.allowUpdate.where({ ownerSubject: session.user_id });
  policy.workspaces.allowDelete.where({ ownerSubject: session.user_id });

  const ownsMembershipWorkspace = (member: RowContext<WorkspaceScoped>) =>
    policy.workspaces.exists.where({
      id: member.workspaceId,
      ownerSubject: session.user_id,
    });
  policy.members.allowRead.where((member) =>
    anyOf([membership(member), ownsMembershipWorkspace(member)]),
  );
  policy.members.allowInsert.where(ownsMembershipWorkspace);
  policy.members.allowUpdate.where(ownsMembershipWorkspace);
  policy.members.allowDelete.where(ownsMembershipWorkspace);

  policy.pages.allowRead.where(membership);
  policy.pages.allowInsert.where(management);
  policy.pages.allowUpdate.where(management);
  policy.pages.allowDelete.where(management);
  policy.blocks.allowRead.where(membership);
  policy.blocks.allowInsert.where(management);
  policy.blocks.allowUpdate.where(management);
  policy.blocks.allowDelete.where(management);
  policy.tasks.allowRead.where(membership);
  policy.tasks.allowInsert.where(management);
  policy.tasks.allowUpdate.where(management);
  policy.tasks.allowDelete.where(management);
  policy.calendarEvents.allowRead.where(membership);
  policy.calendarEvents.allowInsert.where(management);
  policy.calendarEvents.allowUpdate.where(management);
  policy.calendarEvents.allowDelete.where(management);
  policy.songs.allowRead.where(membership);
  policy.songs.allowInsert.where(management);
  policy.songs.allowUpdate.where(management);
  policy.songs.allowDelete.where(management);
  policy.attachments.allowRead.where(membership);
  policy.attachments.allowInsert.where(management);
  policy.attachments.allowUpdate.where(management);
  policy.attachments.allowDelete.where(management);

  policy.suggestions.allowRead.where(membership);
  policy.suggestions.allowInsert.where(membership);
  policy.suggestions.allowUpdate.where(management);
  policy.suggestions.allowDelete.where(management);
});
