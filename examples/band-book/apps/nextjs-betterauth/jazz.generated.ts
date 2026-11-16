// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type JsonValue } from "jazz-tools";

export type BlockKind = "paragraph" | "heading" | "todo" | "bullet" | "quote" | "divider" | "image" | "file";
export type InviteRole = "member" | "viewer" | "editor";
export type IssueStatus = "backlog" | "todo" | "in_progress" | "done";
export type IssuePriority = "none" | "low" | "medium" | "high" | "urgent";
export type MemberRole = "owner" | "member" | "viewer" | "guest";
export type PageGrantRole = "editor" | "viewer";
export type PageKind = "doc" | "issues" | "issue" | "song";

export type Attachment = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  name: string;
  mimeType: string;
  byteLength: number;
  bytes: Uint8Array;
};

export type AttachmentInsert = {
  workspaceId: string;
  pageId: string;
  name: string;
  mimeType: string;
  byteLength: number;
  bytes: Uint8Array;
};

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress: string | null;
  userAgent: string | null;
  userId: string;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string | null;
  userAgent?: string | null;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type Block = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  parentBlockId: string | null;
  position: number;
  kind: BlockKind;
  text: string;
  checked: boolean;
  attachmentId: string | null;
};

export type BlockInsert = {
  workspaceId: string;
  pageId: string;
  parentBlockId?: string | null;
  position: number;
  kind: BlockKind;
  text: string;
  checked: boolean;
  attachmentId?: string | null;
};

export type Invite = {
  readonly id: string;
  workspaceId: string;
  pageId: string | null;
  role: InviteRole;
  token: string;
  label: string;
};

export type InviteInsert = {
  workspaceId: string;
  pageId?: string | null;
  role: InviteRole;
  token: string;
  label: string;
};

export type Issue = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  databaseId: string;
  status: IssueStatus;
  priority: IssuePriority;
  assignee: string | null;
  labels: string[];
};

export type IssueInsert = {
  workspaceId: string;
  pageId: string;
  databaseId: string;
  status: IssueStatus;
  priority: IssuePriority;
  assignee?: string | null;
  labels: string[];
};

export type Member = {
  readonly id: string;
  workspaceId: string;
  account: string;
  displayName: string;
  role: MemberRole;
};

export type MemberInsert = {
  workspaceId: string;
  account: string;
  displayName: string;
  role: MemberRole;
};

export type PageGrant = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  account: string;
  role: PageGrantRole;
};

export type PageGrantInsert = {
  workspaceId: string;
  pageId: string;
  account: string;
  role: PageGrantRole;
};

export type Page = {
  readonly id: string;
  workspaceId: string;
  parentId: string | null;
  title: string;
  kind: PageKind;
};

export type PageInsert = {
  workspaceId: string;
  parentId?: string | null;
  title: string;
  kind: PageKind;
};

export type SheetComment = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  author: string;
  range: JsonValue;
  text: string;
  resolved: boolean;
};

export type SheetCommentInsert = {
  workspaceId: string;
  pageId: string;
  author: string;
  range: JsonValue;
  text: string;
  resolved: boolean;
};

export type SheetPresence = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  account: string;
  cursor: JsonValue;
  selection: JsonValue | null;
  seenAt: number;
};

export type SheetPresenceInsert = {
  workspaceId: string;
  pageId: string;
  account: string;
  cursor: JsonValue;
  selection?: JsonValue | null;
  seenAt: number;
};

export type SongSheet = {
  readonly id: string;
  workspaceId: string;
  pageId: string;
  body: string;
};

export type SongSheetInsert = {
  workspaceId: string;
  pageId: string;
  body: string;
};

export type Workspace = {
  readonly id: string;
  name: string;
};

export type WorkspaceInsert = {
  name: string;
};

interface Types extends AppTypes {
  attachments: {
    row: Attachment;
    insert: AttachmentInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      mimeType: { kind: "string"; nullable: false; value: string };
      byteLength: { kind: "int"; nullable: false; value: number };
      bytes: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  blocks: {
    row: Block;
    insert: BlockInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      parentBlockId: { kind: "uuid"; nullable: true; value: string };
      position: { kind: "float"; nullable: false; value: number };
      kind: { kind: "enum"; nullable: false; value: BlockKind };
      text: { kind: "string"; nullable: false; value: string };
      checked: { kind: "boolean"; nullable: false; value: boolean };
      attachmentId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      attachment: { one: "attachments"; nullable: true };
      page: { one: "pages"; nullable: false };
      parentBlock: { one: "blocks"; nullable: true };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  invites: {
    row: Invite;
    insert: InviteInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: true; value: string };
      role: { kind: "enum"; nullable: false; value: InviteRole };
      token: { kind: "string"; nullable: false; value: string };
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      page: { one: "pages"; nullable: true };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  issues: {
    row: Issue;
    insert: IssueInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      databaseId: { kind: "uuid"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: IssueStatus };
      priority: { kind: "enum"; nullable: false; value: IssuePriority };
      assignee: { kind: "uuid"; nullable: true; value: string };
      labels: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {
      database: { one: "pages"; nullable: false };
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  members: {
    row: Member;
    insert: MemberInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      account: { kind: "uuid"; nullable: false; value: string };
      displayName: { kind: "string"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: MemberRole };
    };
    relations: {
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  pageGrants: {
    row: PageGrant;
    insert: PageGrantInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      account: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: PageGrantRole };
    };
    relations: {
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  pages: {
    row: Page;
    insert: PageInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      parentId: { kind: "uuid"; nullable: true; value: string };
      title: { kind: "string"; nullable: false; value: string };
      kind: { kind: "enum"; nullable: false; value: PageKind };
    };
    relations: {
      blocksViaPage: { many: "blocks" };
      childrenViaParent: { many: "pages" };
      grantsViaPage: { many: "pageGrants" };
      issuesViaPage: { many: "issues" };
      parent: { one: "pages"; nullable: true };
      sheetsViaPage: { many: "songSheets" };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  sheetComments: {
    row: SheetComment;
    insert: SheetCommentInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      author: { kind: "uuid"; nullable: false; value: string };
      range: { kind: "json"; nullable: false; value: JsonValue };
      text: { kind: "string"; nullable: false; value: string };
      resolved: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  sheetPresence: {
    row: SheetPresence;
    insert: SheetPresenceInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      account: { kind: "uuid"; nullable: false; value: string };
      cursor: { kind: "json"; nullable: false; value: JsonValue };
      selection: { kind: "json"; nullable: true; value: JsonValue };
      seenAt: { kind: "float"; nullable: false; value: number };
    };
    relations: {
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  songSheets: {
    row: SongSheet;
    insert: SongSheetInsert;
    columns: {
      workspaceId: { kind: "uuid"; nullable: false; value: string };
      pageId: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      page: { one: "pages"; nullable: false };
      workspace: { one: "workspaces"; nullable: false };
    };
  };
  workspaces: {
    row: Workspace;
    insert: WorkspaceInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      membersViaWorkspace: { many: "members" };
      pagesViaWorkspace: { many: "pages" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:ee57aa162f4e576d9ebc995e16258c42c48125d2b2f8191a63cfe127fe08377a",
  "schema": {
    "current": "553979b0f2b20e85b22e5f83f56318fe11425287aea9f72eca6eb235c5512511",
    "schemas": [
      {
        "hash": "f9c2e1ab9391434b67488cb23410ed340cd4836de7fbc3ce673ed2a56bc07f2c",
        "schema": {
          "tables": {
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mimeType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": false },
              ],
              "indexed_columns": ["pageId"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "issuer", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "blocks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentBlockId",
                  "nullable": true,
                  "references": "blocks",
                },
                { "column_type": { "type": "Double" }, "name": "position", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "paragraph",
                      "heading",
                      "todo",
                      "bullet",
                      "quote",
                      "divider",
                      "image",
                      "file",
                    ],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "checked", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "attachmentId",
                  "nullable": true,
                  "references": "attachments",
                },
              ],
              "indexed_columns": ["pageId", "position"],
            },
            "invites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": true,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["member", "viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
              ],
              "indexed_columns": ["token", "pageId", "workspaceId"],
            },
            "issues": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "databaseId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["backlog", "todo", "in_progress", "done"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["none", "low", "medium", "high", "urgent"],
                  },
                  "name": "priority",
                  "nullable": false,
                },
                { "column_type": { "type": "Uuid" }, "name": "assignee", "nullable": true },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "labels",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["databaseId", "status", "pageId"],
            },
            "members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["owner", "member", "viewer", "guest"],
                  },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["workspaceId", "account"],
            },
            "pageGrants": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["editor", "viewer"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["pageId", "account", "workspaceId"],
            },
            "pages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": true,
                  "references": "pages",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["doc", "issues", "issue", "song"] },
                  "name": "kind",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["workspaceId", "parentId"],
            },
            "sheetComments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "range", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "resolved", "nullable": false },
              ],
              "indexed_columns": ["pageId"],
            },
            "sheetPresence": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "cursor", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "selection", "nullable": true },
                { "column_type": { "type": "Double" }, "name": "seenAt", "nullable": false },
              ],
              "history": "latest",
              "indexed_columns": ["pageId"],
            },
            "songSheets": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Text" },
                  "merge_strategy": "Markdown",
                  "name": "body",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["pageId"],
            },
            "workspaces": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
      {
        "hash": "553979b0f2b20e85b22e5f83f56318fe11425287aea9f72eca6eb235c5512511",
        "schema": {
          "tables": {
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mimeType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": false },
              ],
              "indexed_columns": ["pageId"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "blocks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentBlockId",
                  "nullable": true,
                  "references": "blocks",
                },
                { "column_type": { "type": "Double" }, "name": "position", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "paragraph",
                      "heading",
                      "todo",
                      "bullet",
                      "quote",
                      "divider",
                      "image",
                      "file",
                    ],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "checked", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "attachmentId",
                  "nullable": true,
                  "references": "attachments",
                },
              ],
              "indexed_columns": ["pageId", "position"],
            },
            "invites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": true,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["member", "viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
              ],
              "indexed_columns": ["token", "pageId", "workspaceId"],
            },
            "issues": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "databaseId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["backlog", "todo", "in_progress", "done"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["none", "low", "medium", "high", "urgent"],
                  },
                  "name": "priority",
                  "nullable": false,
                },
                { "column_type": { "type": "Uuid" }, "name": "assignee", "nullable": true },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "labels",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["databaseId", "status", "pageId"],
            },
            "members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["owner", "member", "viewer", "guest"],
                  },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["workspaceId", "account"],
            },
            "pageGrants": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["editor", "viewer"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["pageId", "account", "workspaceId"],
            },
            "pages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": true,
                  "references": "pages",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["doc", "issues", "issue", "song"] },
                  "name": "kind",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["workspaceId", "parentId"],
            },
            "sheetComments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "range", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "resolved", "nullable": false },
              ],
              "indexed_columns": ["pageId"],
            },
            "sheetPresence": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "cursor", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "selection", "nullable": true },
                { "column_type": { "type": "Double" }, "name": "seenAt", "nullable": false },
              ],
              "history": "latest",
              "indexed_columns": ["pageId"],
            },
            "songSheets": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "workspaceId",
                  "nullable": false,
                  "references": "workspaces",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pageId",
                  "nullable": false,
                  "references": "pages",
                },
                {
                  "column_type": { "type": "Text" },
                  "merge_strategy": "Markdown",
                  "name": "body",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["pageId"],
            },
            "workspaces": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "issuer",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "better_auth_account",
          },
        ],
        "fromHash": "f9c2e1ab9391434b67488cb23410ed340cd4836de7fbc3ce673ed2a56bc07f2c",
        "toHash": "553979b0f2b20e85b22e5f83f56318fe11425287aea9f72eca6eb235c5512511",
      },
    ],
  },
  "relations": {
    "attachments": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "blocks": {
      "attachment": { "one": "attachments", "column": "attachmentId" },
      "page": { "one": "pages", "column": "pageId" },
      "parentBlock": { "one": "blocks", "column": "parentBlockId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "invites": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "issues": {
      "database": { "one": "pages", "column": "databaseId" },
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "members": { "workspace": { "one": "workspaces", "column": "workspaceId" } },
    "pageGrants": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "pages": {
      "blocksViaPage": { "many": "blocks", "relation": "page" },
      "childrenViaParent": { "many": "pages", "relation": "parent" },
      "grantsViaPage": { "many": "pageGrants", "relation": "page" },
      "issuesViaPage": { "many": "issues", "relation": "page" },
      "parent": { "one": "pages", "column": "parentId" },
      "sheetsViaPage": { "many": "songSheets", "relation": "page" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "sheetComments": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "sheetPresence": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "songSheets": {
      "page": { "one": "pages", "column": "pageId" },
      "workspace": { "one": "workspaces", "column": "workspaceId" },
    },
    "workspaces": {
      "membersViaWorkspace": { "many": "members", "relation": "workspace" },
      "pagesViaWorkspace": { "many": "pages", "relation": "workspace" },
    },
  },
});
