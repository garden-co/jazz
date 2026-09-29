import { definePermissions, type RowContext } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app, type Invite, type Page, type PageGrant, type WorkspaceRole } from "./schema";

import { PAGE_TREE_MAX_DEPTH } from "./src/lib/limits";

const WORKSPACE_READERS: WorkspaceRole[] = ["owner", "member", "viewer"];
const WORKSPACE_EDITORS: WorkspaceRole[] = ["owner", "member"];

const bandBookPermissions = definePermissions(
  app,
  ({ policy, session, allOf, anyOf, allowedTo }) => {
    const me = session.user.account;
    const depth = { maxDepth: PAGE_TREE_MAX_DEPTH };

    const hasRole = (workspaceId: RowContext<Page>["workspaceId"], roles: WorkspaceRole[]) =>
      policy.members.exists.where({ workspaceId, account: me, role: { in: roles } });

    const sameWorkspaceParent = (page: RowContext<Page>) =>
      anyOf([
        { parentId: null },
        policy.pages.exists.where({ id: page.parentId, workspaceId: page.workspaceId }),
      ]);

    // Access that flows down the tree: a workspace role, or edit access to the
    // parent (which itself may come from a grant further up).
    const editsFromAbove = (page: RowContext<Page>) =>
      anyOf([hasRole(page.workspaceId, WORKSPACE_EDITORS), allowedTo.update("parent", depth)]);
    const grantedEditor = (page: RowContext<Page>) =>
      policy.pageGrants.exists.where({ pageId: page.id, account: me, role: "editor" });

    // Workspaces --------------------------------------------------------------
    policy.workspaces.allowRead.where((workspace) =>
      policy.members.exists.where({ workspaceId: workspace.id, account: me }),
    );
    policy.workspaces.allowInsert.always();
    policy.workspaces.allowUpdate.where((workspace) => hasRole(workspace.id, ["owner"]));
    policy.workspaces.allowDelete.where((workspace) => hasRole(workspace.id, ["owner"]));

    // Members -----------------------------------------------------------------
    // Anyone in a workspace, guests included, can see who else is in it: the
    // roster is what issue assignees and sharing dialogs are picked from.
    policy.members.allowRead.where((member) =>
      anyOf([
        { account: me },
        policy.members.exists.where({ workspaceId: member.workspaceId, account: me }),
      ]),
    );
    policy.members.allowInsert.where((member) =>
      anyOf([
        hasRole(member.workspaceId, ["owner"]),
        // The creator of a workspace makes themselves its first owner.
        allOf([
          { account: me, role: "owner" },
          policy.workspaces.exists.where({ id: member.workspaceId, "$createdBy.account": me }),
        ]),
      ]),
    );
    policy.members.allowUpdate
      .whereOld((member) => hasRole(member.workspaceId, ["owner"]))
      .whereNew((member) =>
        allOf([
          hasRole(member.workspaceId, ["owner"]),
          policy.members.exists.where({
            id: member.id,
            workspaceId: member.workspaceId,
            account: member.account,
          }),
        ]),
      );
    policy.members.allowDelete.where((member) =>
      anyOf([hasRole(member.workspaceId, ["owner"]), { account: me }]),
    );

    // Pages -------------------------------------------------------------------
    policy.pages.allowRead.where((page) =>
      anyOf([
        hasRole(page.workspaceId, WORKSPACE_READERS),
        policy.pageGrants.exists.where({ pageId: page.id, account: me }),
        allowedTo.read("parent", depth),
      ]),
    );
    policy.pages.allowInsert.where((page) =>
      allOf([sameWorkspaceParent(page), editsFromAbove(page)]),
    );
    policy.pages.allowUpdate
      .whereOld((page) => anyOf([editsFromAbove(page), grantedEditor(page)]))
      .whereNew((page) =>
        allOf([
          // A page never changes workspace or kind.
          policy.pages.exists.where({
            id: page.id,
            workspaceId: page.workspaceId,
            kind: page.kind,
          }),
          sameWorkspaceParent(page),
          anyOf([
            // Moving is allowed into any page the editor can edit from above.
            editsFromAbove(page),
            // The root page of an editor grant can be renamed, not moved away.
            allOf([
              grantedEditor(page),
              policy.pages.exists.where({ id: page.id, parentId: page.parentId }),
            ]),
          ]),
        ]),
      );
    // Deleting needs edit access from above, so a grant holder cannot delete
    // the page they were invited to.
    policy.pages.allowDelete.where((page) => editsFromAbove(page));

    // Page grants -------------------------------------------------------------
    // Owners and band members share pages; grant holders see their own grants.
    // Invite links are redeemed by the server, which checks the token itself.
    const sharesPage = (grant: RowContext<PageGrant>) =>
      allOf([
        hasRole(grant.workspaceId, WORKSPACE_EDITORS),
        policy.pages.exists.where({ id: grant.pageId, workspaceId: grant.workspaceId }),
      ]);
    policy.pageGrants.allowRead.where((grant) => anyOf([{ account: me }, sharesPage(grant)]));
    policy.pageGrants.allowInsert.where(sharesPage);
    policy.pageGrants.allowUpdate
      .whereOld(sharesPage)
      .whereNew((grant) =>
        allOf([
          sharesPage(grant),
          policy.pageGrants.exists.where({
            id: grant.id,
            pageId: grant.pageId,
            account: grant.account,
          }),
        ]),
      );
    policy.pageGrants.allowDelete.where((grant) => anyOf([sharesPage(grant), { account: me }]));

    // Blocks and attachments inherit everything from their page ----------------
    policy.blocks.allowRead.where(allowedTo.read("page"));
    policy.blocks.allowInsert.where((block) =>
      allOf([
        allowedTo.update("page"),
        policy.pages.exists.where({ id: block.pageId, workspaceId: block.workspaceId }),
        anyOf([
          { parentBlockId: null },
          policy.blocks.exists.where({ id: block.parentBlockId, pageId: block.pageId }),
        ]),
      ]),
    );
    policy.blocks.allowUpdate.whereOld(allowedTo.update("page")).whereNew((block) =>
      allOf([
        allowedTo.update("page"),
        policy.blocks.exists.where({
          id: block.id,
          pageId: block.pageId,
          workspaceId: block.workspaceId,
        }),
        anyOf([
          { parentBlockId: null },
          policy.blocks.exists.where({ id: block.parentBlockId, pageId: block.pageId }),
        ]),
      ]),
    );
    policy.blocks.allowDelete.where(allowedTo.update("page"));

    policy.attachments.allowRead.where(allowedTo.read("page"));
    policy.attachments.allowInsert.where((attachment) =>
      allOf([
        allowedTo.update("page"),
        policy.pages.exists.where({ id: attachment.pageId, workspaceId: attachment.workspaceId }),
      ]),
    );
    policy.attachments.allowUpdate.never();
    policy.attachments.allowDelete.where(allowedTo.update("page"));

    // Issues ------------------------------------------------------------------
    policy.issues.allowRead.where(allowedTo.read("page"));
    policy.issues.allowInsert.where((issue) =>
      allOf([
        allowedTo.update("page"),
        policy.pages.exists.where({
          id: issue.pageId,
          parentId: issue.databaseId,
          workspaceId: issue.workspaceId,
          kind: "issue",
        }),
        policy.pages.exists.where({ id: issue.databaseId, kind: "issues" }),
      ]),
    );
    policy.issues.allowUpdate.whereOld(allowedTo.update("page")).whereNew((issue) =>
      allOf([
        allowedTo.update("page"),
        policy.issues.exists.where({
          id: issue.id,
          pageId: issue.pageId,
          databaseId: issue.databaseId,
          workspaceId: issue.workspaceId,
        }),
      ]),
    );
    policy.issues.allowDelete.where(allowedTo.update("page"));

    // Invites -----------------------------------------------------------------
    // Workspace invites are for owners; page invites for anyone who shares.
    const managesInvite = (invite: RowContext<Invite>) =>
      anyOf([
        allOf([
          { pageId: null, role: { in: ["member", "viewer"] } },
          hasRole(invite.workspaceId, ["owner"]),
        ]),
        allOf([
          { role: { in: ["editor", "viewer"] } },
          hasRole(invite.workspaceId, WORKSPACE_EDITORS),
          policy.pages.exists.where({ id: invite.pageId, workspaceId: invite.workspaceId }),
        ]),
      ]);
    policy.invites.allowRead.where(managesInvite);
    policy.invites.allowInsert.where(managesInvite);
    policy.invites.allowUpdate.never();
    policy.invites.allowDelete.where(managesInvite);
  },
);

export default { ...betterAuthPermissions, ...bandBookPermissions };
