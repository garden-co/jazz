import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isMember = (workspaceId: s.Uuid) =>
  exists.members({ workspaceId, account: session.user, role: ["owner"] });

const isMember2 = (workspaceId: s.Uuid) =>
  exists.members({ workspaceId, account: session.user, role: ["owner", "member"] });

const accountIsMe = { account: session.user };

const isMember3 = (workspaceId: s.Uuid) => exists.members({ workspaceId, account: session.user });

const isPageGrant = (pageId: s.Uuid) =>
  exists.pageGrants({ pageId, account: session.user, role: "editor" });

const authorIsMe = { author: session.user };

export const permissions = definePermissions({
  ...betterAuthPermissions,
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
    read: (invite) =>
      anyOf(
        allOf({ pageId: null, role: ["member", "viewer"] }, isMember(invite.workspaceId)),
        allOf(
          { role: ["editor", "viewer"] },
          isMember2(invite.workspaceId),
          invite.page.where({ workspaceId: invite.workspaceId }),
        ),
      ),
    insert: (invite) =>
      anyOf(
        allOf({ pageId: null, role: ["member", "viewer"] }, isMember(invite.workspaceId)),
        allOf(
          { role: ["editor", "viewer"] },
          isMember2(invite.workspaceId),
          invite.page.where({ workspaceId: invite.workspaceId }),
        ),
      ),
    update: false,
    delete: (invite) =>
      anyOf(
        allOf({ pageId: null, role: ["member", "viewer"] }, isMember(invite.workspaceId)),
        allOf(
          { role: ["editor", "viewer"] },
          isMember2(invite.workspaceId),
          invite.page.where({ workspaceId: invite.workspaceId }),
        ),
      ),
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
    read: (member) => anyOf(accountIsMe, isMember3(member.workspaceId)),
    insert: (member) =>
      anyOf(
        isMember(member.workspaceId),
        allOf(
          { account: session.user, role: "owner" },
          member.workspace.where({ $createdBy: session.user }),
        ),
      ),
    updateIf: (member) => isMember(member.workspaceId),
    updateTo: (member) =>
      allOf(
        isMember(member.workspaceId),
        exists.members({ id: member.id, workspaceId: member.workspaceId, account: member.account }),
      ),
    delete: (member) => anyOf(isMember(member.workspaceId), accountIsMe),
  },
  pageGrants: {
    read: (pageGrant) =>
      anyOf(
        accountIsMe,
        allOf(
          isMember2(pageGrant.workspaceId),
          pageGrant.page.where({ workspaceId: pageGrant.workspaceId }),
        ),
      ),
    insert: (pageGrant) =>
      allOf(
        isMember2(pageGrant.workspaceId),
        pageGrant.page.where({ workspaceId: pageGrant.workspaceId }),
      ),
    updateIf: (pageGrant) =>
      allOf(
        isMember2(pageGrant.workspaceId),
        pageGrant.page.where({ workspaceId: pageGrant.workspaceId }),
      ),
    updateTo: (pageGrant) =>
      allOf(
        allOf(
          isMember2(pageGrant.workspaceId),
          pageGrant.page.where({ workspaceId: pageGrant.workspaceId }),
        ),
        exists.pageGrants({
          id: pageGrant.id,
          pageId: pageGrant.pageId,
          account: pageGrant.account,
        }),
      ),
    delete: (pageGrant) =>
      anyOf(
        allOf(
          isMember2(pageGrant.workspaceId),
          pageGrant.page.where({ workspaceId: pageGrant.workspaceId }),
        ),
        accountIsMe,
      ),
  },
  pages: {
    read: (page) =>
      anyOf(
        exists.members({
          workspaceId: page.workspaceId,
          account: session.user,
          role: ["owner", "member", "viewer"],
        }),
        exists.pageGrants({ pageId: page.id, account: session.user }),
        allowedTo.read(page.parent, { maxDepth: 8 }),
      ),
    insert: (page) =>
      allOf(
        anyOf({ parentId: null }, page.parent.where({ workspaceId: page.workspaceId })),
        anyOf(isMember2(page.workspaceId), allowedTo.update(page.parent, { maxDepth: 8 })),
      ),
    updateIf: (page) =>
      anyOf(
        anyOf(isMember2(page.workspaceId), allowedTo.update(page.parent, { maxDepth: 8 })),
        isPageGrant(page.id),
      ),
    updateTo: (page, old) =>
      allOf(
        exists.pages({ id: page.id, workspaceId: page.workspaceId, kind: page.kind }),
        anyOf({ parentId: null }, page.parent.where({ workspaceId: page.workspaceId })),
        anyOf(
          anyOf(isMember2(page.workspaceId), allowedTo.update(page.parent, { maxDepth: 8 })),
          allOf(isPageGrant(page.id), { parentId: old.parentId }),
        ),
      ),
    delete: (page) =>
      anyOf(isMember2(page.workspaceId), allowedTo.update(page.parent, { maxDepth: 8 })),
  },
  sheetComments: {
    read: (sheetComment) => allowedTo.read(sheetComment.page),
    insert: (sheetComment) =>
      allOf(
        authorIsMe,
        allowedTo.read(sheetComment.page),
        sheetComment.page.where({ workspaceId: sheetComment.workspaceId, kind: "song" }),
      ),
    updateIf: (sheetComment) => anyOf(authorIsMe, allowedTo.update(sheetComment.page)),
    updateTo: (sheetComment) =>
      allOf(
        allowedTo.read(sheetComment.page),
        exists.sheetComments({
          id: sheetComment.id,
          pageId: sheetComment.pageId,
          workspaceId: sheetComment.workspaceId,
          author: sheetComment.author,
        }),
      ),
    delete: (sheetComment) => anyOf(authorIsMe, allowedTo.update(sheetComment.page)),
  },
  sheetPresence: {
    read: (sheetPresence) => allowedTo.read(sheetPresence.page),
    insert: (sheetPresence) =>
      allOf(
        accountIsMe,
        allowedTo.read(sheetPresence.page),
        sheetPresence.page.where({ workspaceId: sheetPresence.workspaceId, kind: "song" }),
      ),
    updateIf: accountIsMe,
    updateTo: (sheetPresence) =>
      allOf(
        accountIsMe,
        exists.sheetPresence({
          id: sheetPresence.id,
          pageId: sheetPresence.pageId,
          workspaceId: sheetPresence.workspaceId,
        }),
      ),
    delete: (sheetPresence) => anyOf(accountIsMe, allowedTo.update(sheetPresence.page)),
  },
  songSheets: {
    read: (songSheet) => allowedTo.read(songSheet.page),
    insert: (songSheet) =>
      allOf(
        allowedTo.update(songSheet.page),
        songSheet.page.where({ workspaceId: songSheet.workspaceId, kind: "song" }),
      ),
    updateIf: (songSheet) => allowedTo.update(songSheet.page),
    updateTo: (songSheet) =>
      allOf(
        allowedTo.update(songSheet.page),
        exists.songSheets({
          id: songSheet.id,
          pageId: songSheet.pageId,
          workspaceId: songSheet.workspaceId,
        }),
      ),
    delete: (songSheet) => allowedTo.update(songSheet.page),
  },
  workspaces: {
    read: (workspace) => isMember3(workspace.id),
    insert: true,
    update: (workspace) => isMember(workspace.id),
    delete: (workspace) => isMember(workspace.id),
  },
});
