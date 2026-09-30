import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

/**
 * BandBook is a Notion-style workspace: a tree of pages, each holding an
 * ordered tree of blocks. One page kind is a database of issues whose rows are
 * themselves pages.
 *
 * Every content row carries its `workspaceId`. Access still comes from the
 * page tree (see permissions.ts): the column only lets policies check that a
 * child and its parent belong to the same workspace, so no write can graft a
 * page from one band onto another band's tree.
 */
const schema = {
  ...betterAuthSchema,
  workspaces: s.table(
    { name: s.string() },
    {
      membersViaWorkspace: s.reverse("members", "workspace"),
      pagesViaWorkspace: s.reverse("pages", "workspace"),
    },
  ),
  // Workspace-wide roles. `guest` rows only carry a display name for people
  // whose access comes from page grants; the role itself grants nothing.
  members: s
    .table(
      {
        workspaceId: s.uuid(),
        account: s.uuid(),
        displayName: s.string(),
        role: s.enum("owner", "member", "viewer", "guest"),
      },
      { workspace: s.rel("workspaces", "workspaceId") },
    )
    .indexOnly(["workspaceId", "account"]),
  pages: s
    .table(
      {
        workspaceId: s.uuid(),
        parentId: s.uuid().optional(),
        title: s.string(),
        kind: s.enum("doc", "issues", "issue"),
      },
      {
        workspace: s.rel("workspaces", "workspaceId"),
        parent: s.rel("pages", "parentId"),
        childrenViaParent: s.reverse("pages", "parent"),
        blocksViaPage: s.reverse("blocks", "page"),
        grantsViaPage: s.reverse("pageGrants", "page"),
        issuesViaPage: s.reverse("issues", "page"),
      },
    )
    .indexOnly(["workspaceId", "parentId"]),
  // A grant on one page reaches every descendant page and its blocks.
  pageGrants: s
    .table(
      {
        workspaceId: s.uuid(),
        pageId: s.uuid(),
        account: s.uuid(),
        role: s.enum("editor", "viewer"),
      },
      { workspace: s.rel("workspaces", "workspaceId"), page: s.rel("pages", "pageId") },
    )
    .indexOnly(["pageId", "account", "workspaceId"]),
  // Blocks are ordered by a fractional `position` inside their parent block
  // (or the page when `parentBlockId` is null), so an insert between two
  // blocks never rewrites its siblings.
  blocks: s
    .table(
      {
        workspaceId: s.uuid(),
        pageId: s.uuid(),
        parentBlockId: s.uuid().optional(),
        position: s.float(),
        kind: s.enum("paragraph", "heading", "todo", "bullet", "quote", "divider", "image", "file"),
        text: s.string(),
        checked: s.boolean(),
        attachmentId: s.uuid().optional(),
      },
      {
        workspace: s.rel("workspaces", "workspaceId"),
        page: s.rel("pages", "pageId"),
        parentBlock: s.rel("blocks", "parentBlockId"),
        attachment: s.rel("attachments", "attachmentId"),
      },
    )
    .indexOnly(["pageId", "position"]),
  // Attachment bytes are a large value written with `insertStreaming`. List
  // queries select the metadata columns only, so a page never downloads a
  // file it does not render.
  attachments: s
    .table(
      {
        workspaceId: s.uuid(),
        pageId: s.uuid(),
        name: s.string(),
        mimeType: s.string(),
        byteLength: s.int(),
        bytes: s.bytes(),
      },
      { workspace: s.rel("workspaces", "workspaceId"), page: s.rel("pages", "pageId") },
    )
    .indexOnly(["pageId"]),
  // Database properties of an issue. The title and body live on the issue's
  // own page, a child of the issues database page.
  issues: s
    .table(
      {
        workspaceId: s.uuid(),
        pageId: s.uuid(),
        databaseId: s.uuid(),
        status: s.enum("backlog", "todo", "in_progress", "done"),
        priority: s.enum("none", "low", "medium", "high", "urgent"),
        assignee: s.uuid().optional(),
        labels: s.array(s.string()),
      },
      {
        workspace: s.rel("workspaces", "workspaceId"),
        page: s.rel("pages", "pageId"),
        database: s.rel("pages", "databaseId"),
      },
    )
    .indexOnly(["databaseId", "status", "pageId"]),
  // An invite is a bearer capability. Only people who may share its scope can
  // read or create it. The server redeems a token on the invitee's behalf
  // (app/api/invites/redeem), so tokens never reach anyone else's client.
  invites: s
    .table(
      {
        workspaceId: s.uuid(),
        pageId: s.uuid().optional(),
        role: s.enum("member", "viewer", "editor"),
        token: s.string(),
        label: s.string(),
      },
      { workspace: s.rel("workspaces", "workspaceId"), page: s.rel("pages", "pageId") },
    )
    .indexOnly(["token", "pageId", "workspaceId"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Workspace = s.RowOf<typeof app.workspaces>;
export type Member = s.RowOf<typeof app.members>;
export type Page = s.RowOf<typeof app.pages>;
export type PageGrant = s.RowOf<typeof app.pageGrants>;
export type Block = s.RowOf<typeof app.blocks>;
export type Attachment = s.RowOf<typeof app.attachments>;
export type Issue = s.RowOf<typeof app.issues>;
export type Invite = s.RowOf<typeof app.invites>;
export type WorkspaceRole = Member["role"];
export type GrantRole = PageGrant["role"];
export type BlockKind = Block["kind"];
export type IssueStatus = Issue["status"];
export type IssuePriority = Issue["priority"];
