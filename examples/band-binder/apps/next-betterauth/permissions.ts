import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  type WorkspaceScoped = { workspaceId: string };
  const membership = (row: RowContext<WorkspaceScoped>) =>
    policy.members.exists.where({ workspaceId: row.workspaceId, author: session.author });
  const management = (row: RowContext<WorkspaceScoped>) =>
    policy.members.exists.where({
      workspaceId: row.workspaceId,
      author: session.author,
      role: { in: ["owner", "stage_manager"] },
    });
  type PageScoped = WorkspaceScoped & { parentPageId: string | null };
  const validPageWrite = (page: RowContext<PageScoped>) =>
    anyOf([
      allOf([management(page), { parentPageId: null }]),
      allOf([
        management(page),
        policy.exists(policy.pages.where({ id: page.parentPageId, workspaceId: page.workspaceId })),
      ]),
    ]);

  type BlockScoped = WorkspaceScoped & { pageId: string; parentBlockId: string | null };
  const validBlockWrite = (block: RowContext<BlockScoped>) =>
    anyOf([
      allOf([
        management(block),
        policy.exists(policy.pages.where({ id: block.pageId, workspaceId: block.workspaceId })),
        { parentBlockId: null },
      ]),
      allOf([
        management(block),
        policy.exists(policy.pages.where({ id: block.pageId, workspaceId: block.workspaceId })),
        policy.exists(
          policy.blocks.where({
            id: block.parentBlockId,
            workspaceId: block.workspaceId,
            pageId: block.pageId,
          }),
        ),
      ]),
    ]);

  type BlockDependent = WorkspaceScoped & { blockId: string };
  const blockWithMembership = (row: RowContext<BlockDependent>, managerOnly: boolean) =>
    allOf([
      managerOnly ? management(row) : membership(row),
      policy.exists(policy.blocks.where({ id: row.blockId, workspaceId: row.workspaceId })),
    ]);
  const managedBlock = (row: RowContext<BlockDependent>) => blockWithMembership(row, true);
  const memberBlock = (row: RowContext<BlockDependent>) => blockWithMembership(row, false);

  policy.workspaces.allowRead.where((workspace) =>
    anyOf([
      { $createdBy: session.author },
      policy.members.exists.where({ workspaceId: workspace.id, author: session.author }),
    ]),
  );
  policy.workspaces.allowInsert.always();
  policy.workspaces.allowUpdate.where({ $createdBy: session.author });
  policy.workspaces.allowDelete.where({ $createdBy: session.author });

  // A member must be able to discover their own grant without first reading
  // the workspace that the grant unlocks. Managers can still inspect the
  // complete roster through the workspace policy.
  policy.members.allowRead.where(
    anyOf([{ author: session.author }, allowedTo.read("workspaceId")]),
  );
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
