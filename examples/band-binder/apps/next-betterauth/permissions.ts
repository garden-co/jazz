import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  type WorkspaceScoped = { workspaceId: string };
  const membership = (row: RowContext<WorkspaceScoped>) =>
    policy.members.exists.where({ workspaceId: row.workspaceId, subject: session.user_id });
  const management = (row: RowContext<WorkspaceScoped>) =>
    policy.members.exists.where({
      workspaceId: row.workspaceId,
      subject: session.user_id,
      role: { in: ["owner", "stage_manager"] },
    });
  const managerJoin = () => ({
    "members.subject": session.user_id,
    "members.role": { in: ["owner", "stage_manager"] as ("owner" | "stage_manager")[] },
  });

  type PageScoped = WorkspaceScoped & { parentPageId: string | null };
  const managedParentPage = (page: RowContext<PageScoped>) =>
    policy.exists(
      policy.pages
        .where({})
        .join(policy.members, { left: "workspaceId", right: "workspaceId" })
        .where({
          "members.workspaceId": page.workspaceId,
          "pages.id": page.parentPageId,
          ...managerJoin(),
        }),
    );
  const validPageWrite = (page: RowContext<PageScoped>) =>
    anyOf([allOf([management(page), { parentPageId: null }]), managedParentPage(page)]);

  type BlockScoped = WorkspaceScoped & { pageId: string; parentBlockId: string | null };
  const managedBlockPage = (block: RowContext<BlockScoped>) =>
    policy.exists(
      policy.pages
        .where({})
        .join(policy.members, { left: "workspaceId", right: "workspaceId" })
        .where({
          "members.workspaceId": block.workspaceId,
          "pages.id": block.pageId,
          ...managerJoin(),
        }),
    );
  const managedParentBlock = (block: RowContext<BlockScoped>) =>
    policy.exists(
      policy.blocks
        .where({})
        .join(policy.members, { left: "workspaceId", right: "workspaceId" })
        .where({
          "members.workspaceId": block.workspaceId,
          "blocks.id": block.parentBlockId,
          "blocks.pageId": block.pageId,
          ...managerJoin(),
        }),
    );
  const validBlockWrite = (block: RowContext<BlockScoped>) =>
    anyOf([allOf([managedBlockPage(block), { parentBlockId: null }]), managedParentBlock(block)]);

  type BlockDependent = WorkspaceScoped & { blockId: string };
  const blockWithMembership = (row: RowContext<BlockDependent>, managerOnly: boolean) =>
    policy.exists(
      policy.blocks
        .where({})
        .join(policy.members, { left: "workspaceId", right: "workspaceId" })
        .where({
          "members.workspaceId": row.workspaceId,
          "blocks.id": row.blockId,
          ...(managerOnly ? managerJoin() : { "members.subject": session.user_id }),
        }),
    );
  const managedBlock = (row: RowContext<BlockDependent>) => blockWithMembership(row, true);
  const memberBlock = (row: RowContext<BlockDependent>) => blockWithMembership(row, false);

  policy.workspaces.allowRead.where((workspace) =>
    anyOf([
      { ownerSubject: session.user_id },
      policy.members.exists.where({ workspaceId: workspace.id, subject: session.user_id }),
    ]),
  );
  policy.workspaces.allowInsert.where({ ownerSubject: session.user_id });
  policy.workspaces.allowUpdate.where({ ownerSubject: session.user_id });
  policy.workspaces.allowDelete.where({ ownerSubject: session.user_id });

  policy.members.allowRead.where(allowedTo.read("workspaceId"));
  policy.members.allowInsert.where(allowedTo.update("workspaceId"));
  policy.members.allowUpdate.where(allowedTo.update("workspaceId"));
  policy.members.allowDelete.where(allowedTo.update("workspaceId"));

  policy.pages.allowRead.where(membership);
  policy.pages.allowInsert.where(validPageWrite);
  policy.pages.allowUpdate.whereOld(validPageWrite).whereNew(validPageWrite);
  policy.pages.allowDelete.where(management);
  policy.blocks.allowRead.where(membership);
  policy.blocks.allowInsert.where(validBlockWrite);
  policy.blocks.allowUpdate.whereOld(validBlockWrite).whereNew(validBlockWrite);
  policy.blocks.allowDelete.where(management);

  policy.tasks.allowRead.where(membership);
  policy.tasks.allowInsert.where(managedBlock);
  policy.tasks.allowUpdate.whereOld(managedBlock).whereNew(managedBlock);
  policy.tasks.allowDelete.where(management);
  policy.calendarEvents.allowRead.where(membership);
  policy.calendarEvents.allowInsert.where(managedBlock);
  policy.calendarEvents.allowUpdate.whereOld(managedBlock).whereNew(managedBlock);
  policy.calendarEvents.allowDelete.where(management);
  policy.songs.allowRead.where(membership);
  policy.songs.allowInsert.where(managedBlock);
  policy.songs.allowUpdate.whereOld(managedBlock).whereNew(managedBlock);
  policy.songs.allowDelete.where(management);
  policy.attachments.allowRead.where(membership);
  policy.attachments.allowInsert.where(managedBlock);
  policy.attachments.allowUpdate.whereOld(managedBlock).whereNew(managedBlock);
  policy.attachments.allowDelete.where(management);

  policy.suggestions.allowRead.where(membership);
  policy.suggestions.allowInsert.where(memberBlock);
  policy.suggestions.allowUpdate.whereOld(managedBlock).whereNew(managedBlock);
  policy.suggestions.allowDelete.where(management);
});
