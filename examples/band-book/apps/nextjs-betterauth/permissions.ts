import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { PAGE_TREE_MAX_DEPTH } from "./src/lib/limits";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

type WorkspaceRole = "owner" | "member" | "viewer" | "guest";
type Page = s.Row<typeof schema, "pages">;

const WORKSPACE_READERS: WorkspaceRole[] = ["owner", "member", "viewer"];
const WORKSPACE_EDITORS: WorkspaceRole[] = ["owner", "member"];

const depth = { maxDepth: PAGE_TREE_MAX_DEPTH };

const isWorkspaceMember = (workspaceId: s.Uuid) =>
  exists.members({ workspaceId, account: session.user });

const hasRole = (workspaceId: s.Uuid, role: WorkspaceRole[]) =>
  exists.members({ workspaceId, account: session.user, role });

const isGrantedEditor = (pageId: s.Uuid) =>
  exists.pageGrants({ pageId, account: session.user, role: "editor" });

const mine = { account: session.user };

const authoredByMe = { author: session.user };

const sameWorkspaceParent = (page: Page) =>
  anyOf({ parentId: null }, page.parent.where({ workspaceId: page.workspaceId }));

// Access that flows down the tree: a workspace role, or edit access to the
// parent (which itself may come from a grant further up).
const editsFromAbove = (page: Page) =>
  anyOf(hasRole(page.workspaceId, WORKSPACE_EDITORS), allowedTo.update(page.parent, depth));

// Owners and band members share pages; grant holders see their own grants.
// Invite links are redeemed by the server, which checks the token itself.
const sharesPage = (grant: s.Row<typeof schema, "pageGrants">) =>
  allOf(
    hasRole(grant.workspaceId, WORKSPACE_EDITORS),
    grant.page.where({ workspaceId: grant.workspaceId }),
  );

// Workspace invites are for owners; page invites for anyone who shares.
const managesInvite = (invite: s.Row<typeof schema, "invites">) =>
  anyOf(
    allOf({ pageId: null, role: ["member", "viewer"] }, hasRole(invite.workspaceId, ["owner"])),
    allOf(
      { role: ["editor", "viewer"] },
      hasRole(invite.workspaceId, WORKSPACE_EDITORS),
      invite.page.where({ workspaceId: invite.workspaceId }),
    ),
  );

export const permissions = definePermissions({
  ...betterAuthPermissions,
  // Blocks and attachments inherit everything from their page.
  attachments: {
    read: (attachment) => allowedTo.read(attachment.page),
    insert: (attachment) =>
      allOf(
        allowedTo.update(attachment.page),
        attachment.page.where({ workspaceId: attachment.workspaceId }),
      ),
    update: false,
    delete: (attachment) => allowedTo.update(attachment.page),
  },
  blocks: {
    read: (block) => allowedTo.read(block.page),
    insert: (block) =>
      allOf(
        allowedTo.update(block.page),
        block.page.where({ workspaceId: block.workspaceId }),
        anyOf({ parentBlockId: null }, block.parentBlock.where({ pageId: block.pageId })),
      ),
    updateIf: (block) => allowedTo.update(block.page),
    updateTo: (block) =>
      allOf(
        allowedTo.update(block.page),
        exists.blocks({ id: block.id, pageId: block.pageId, workspaceId: block.workspaceId }),
        anyOf({ parentBlockId: null }, block.parentBlock.where({ pageId: block.pageId })),
      ),
    delete: (block) => allowedTo.update(block.page),
  },
  invites: {
    read: managesInvite,
    insert: managesInvite,
    update: false,
    delete: managesInvite,
  },
  issues: {
    read: (issue) => allowedTo.read(issue.page),
    insert: (issue) =>
      allOf(
        allowedTo.update(issue.page),
        issue.page.where({
          parentId: issue.databaseId,
          workspaceId: issue.workspaceId,
          kind: "issue",
        }),
        issue.database.where({ kind: "issues" }),
      ),
    updateIf: (issue) => allowedTo.update(issue.page),
    updateTo: (issue) =>
      allOf(
        allowedTo.update(issue.page),
        exists.issues({
          id: issue.id,
          pageId: issue.pageId,
          databaseId: issue.databaseId,
          workspaceId: issue.workspaceId,
        }),
      ),
    delete: (issue) => allowedTo.update(issue.page),
  },
  members: {
    // Anyone in a workspace, guests included, can see who else is in it: the
    // roster is what issue assignees and sharing dialogs are picked from.
    read: (member) => anyOf(mine, isWorkspaceMember(member.workspaceId)),
    insert: (member) =>
      anyOf(
        hasRole(member.workspaceId, ["owner"]),
        // The creator of a workspace makes themselves its first owner.
        allOf(
          { account: session.user, role: "owner" },
          member.workspace.where({ "$createdBy.account": session.user }),
        ),
      ),
    updateIf: (member) => hasRole(member.workspaceId, ["owner"]),
    updateTo: (member) =>
      allOf(
        hasRole(member.workspaceId, ["owner"]),
        exists.members({ id: member.id, workspaceId: member.workspaceId, account: member.account }),
      ),
    delete: (member) => anyOf(hasRole(member.workspaceId, ["owner"]), mine),
  },
  pageGrants: {
    read: (grant) => anyOf(mine, sharesPage(grant)),
    insert: sharesPage,
    updateIf: sharesPage,
    updateTo: (grant) =>
      allOf(
        sharesPage(grant),
        exists.pageGrants({ id: grant.id, pageId: grant.pageId, account: grant.account }),
      ),
    delete: (grant) => anyOf(sharesPage(grant), mine),
  },
  pages: {
    read: (page) =>
      anyOf(
        hasRole(page.workspaceId, WORKSPACE_READERS),
        exists.pageGrants({ pageId: page.id, account: session.user }),
        allowedTo.read(page.parent, depth),
      ),
    insert: (page) => allOf(sameWorkspaceParent(page), editsFromAbove(page)),
    updateIf: (page) => anyOf(editsFromAbove(page), isGrantedEditor(page.id)),
    updateTo: (page, old) =>
      allOf(
        // A page never changes workspace or kind.
        exists.pages({ id: page.id, workspaceId: page.workspaceId, kind: page.kind }),
        sameWorkspaceParent(page),
        anyOf(
          // Moving is allowed into any page the editor can edit from above.
          editsFromAbove(page),
          // The root page of an editor grant can be renamed, not moved away.
          allOf(isGrantedEditor(page.id), { parentId: old.parentId }),
        ),
      ),
    // Deleting needs edit access from above, so a grant holder cannot delete
    // the page they were invited to.
    delete: editsFromAbove,
  },
  // The sheet's text belongs to its page. Anyone who can read the page can
  // comment and be seen there; comments are changed by their author or by
  // the page's editors, and presence rows only by their own account.
  sheetComments: {
    read: (comment) => allowedTo.read(comment.page),
    insert: (comment) =>
      allOf(
        authoredByMe,
        allowedTo.read(comment.page),
        comment.page.where({ workspaceId: comment.workspaceId, kind: "song" }),
      ),
    updateIf: (comment) => anyOf(authoredByMe, allowedTo.update(comment.page)),
    updateTo: (comment) =>
      allOf(
        allowedTo.read(comment.page),
        exists.sheetComments({
          id: comment.id,
          pageId: comment.pageId,
          workspaceId: comment.workspaceId,
          author: comment.author,
        }),
      ),
    delete: (comment) => anyOf(authoredByMe, allowedTo.update(comment.page)),
  },
  sheetPresence: {
    read: (presence) => allowedTo.read(presence.page),
    insert: (presence) =>
      allOf(
        mine,
        allowedTo.read(presence.page),
        presence.page.where({ workspaceId: presence.workspaceId, kind: "song" }),
      ),
    updateIf: mine,
    updateTo: (presence) =>
      allOf(
        mine,
        exists.sheetPresence({
          id: presence.id,
          pageId: presence.pageId,
          workspaceId: presence.workspaceId,
        }),
      ),
    // Page editors can clear presence too, so deleting a page leaves nothing.
    delete: (presence) => anyOf(mine, allowedTo.update(presence.page)),
  },
  songSheets: {
    read: (sheet) => allowedTo.read(sheet.page),
    insert: (sheet) =>
      allOf(
        allowedTo.update(sheet.page),
        sheet.page.where({ workspaceId: sheet.workspaceId, kind: "song" }),
      ),
    updateIf: (sheet) => allowedTo.update(sheet.page),
    updateTo: (sheet) =>
      allOf(
        allowedTo.update(sheet.page),
        exists.songSheets({ id: sheet.id, pageId: sheet.pageId, workspaceId: sheet.workspaceId }),
      ),
    delete: (sheet) => allowedTo.update(sheet.page),
  },
  workspaces: {
    read: (workspace) => isWorkspaceMember(workspace.id),
    insert: true,
    update: (workspace) => hasRole(workspace.id, ["owner"]),
    delete: (workspace) => hasRole(workspace.id, ["owner"]),
  },
});
