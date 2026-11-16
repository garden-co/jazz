// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 1ff57864466d8536. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ActivityKind = "created" | "moved" | "assigned" | "renamed" | "commented" | "deleted";
export type ShowCrewRole = "chief" | "crew";
export type TaskStatus = "todo" | "doing" | "done" | "blocked";

export type Activity = {
  readonly id: string;
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  detail: string | null;
};

export type ActivityInsert = {
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  detail?: string | null;
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

export type ChecklistItem = {
  readonly id: string;
  title: string;
  done: boolean;
  ownerAccount: string;
};

export type ChecklistItemInsert = {
  title: string;
  done: boolean;
  ownerAccount: string;
};

export type Comment = {
  readonly id: string;
  taskId: string;
  authorId: string;
  body: string;
};

export type CommentInsert = {
  taskId: string;
  authorId: string;
  body: string;
};

export type Crew = {
  readonly id: string;
  account: string;
  name: string;
};

export type CrewInsert = {
  account: string;
  name: string;
};

export type ShowCrew = {
  readonly id: string;
  showId: string;
  crewId: string;
  account: string;
  role: ShowCrewRole;
  inviteCode: string | null;
};

export type ShowCrewInsert = {
  showId: string;
  crewId: string;
  account: string;
  role: ShowCrewRole;
  inviteCode?: string | null;
};

export type ShowInvite = {
  readonly id: string;
  showId: string;
  code: string;
};

export type ShowInviteInsert = {
  showId: string;
  code: string;
};

export type Show = {
  readonly id: string;
  name: string;
  venue: string;
  date: string;
  doors: string;
  chiefAccount: string;
};

export type ShowInsert = {
  name: string;
  venue: string;
  date: string;
  doors: string;
  chiefAccount: string;
};

export type Task = {
  readonly id: string;
  showId: string;
  title: string;
  status: TaskStatus;
  assigneeId: string | null;
  notes: string | null;
  rank: number;
};

export type TaskInsert = {
  showId: string;
  title: string;
  status: TaskStatus;
  assigneeId?: string | null;
  notes?: string | null;
  rank: number;
};

interface Types extends AppTypes {
  activity: {
    row: Activity;
    insert: ActivityInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      taskId: { kind: "uuid"; nullable: false; value: string };
      actorId: { kind: "uuid"; nullable: false; value: string };
      kind: { kind: "enum"; nullable: false; value: ActivityKind };
      detail: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      actor: { one: "crew"; nullable: false };
      show: { one: "shows"; nullable: false };
      task: { one: "tasks"; nullable: false };
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
  checklistItems: {
    row: ChecklistItem;
    insert: ChecklistItemInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {};
  };
  comments: {
    row: Comment;
    insert: CommentInsert;
    columns: {
      taskId: { kind: "uuid"; nullable: false; value: string };
      authorId: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      author: { one: "crew"; nullable: false };
      task: { one: "tasks"; nullable: false };
    };
  };
  crew: {
    row: Crew;
    insert: CrewInsert;
    columns: {
      account: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      assignedTasks: { many: "tasks" };
      memberships: { many: "showCrew" };
    };
  };
  showCrew: {
    row: ShowCrew;
    insert: ShowCrewInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      crewId: { kind: "uuid"; nullable: false; value: string };
      account: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: ShowCrewRole };
      inviteCode: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      crew: { one: "crew"; nullable: false };
      show: { one: "shows"; nullable: false };
    };
  };
  showInvites: {
    row: ShowInvite;
    insert: ShowInviteInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      show: { one: "shows"; nullable: false };
    };
  };
  shows: {
    row: Show;
    insert: ShowInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      venue: { kind: "string"; nullable: false; value: string };
      date: { kind: "string"; nullable: false; value: string };
      doors: { kind: "string"; nullable: false; value: string };
      chiefAccount: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      activity: { many: "activity" };
      crew: { many: "showCrew" };
      invites: { many: "showInvites" };
      tasks: { many: "tasks" };
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      showId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: TaskStatus };
      assigneeId: { kind: "uuid"; nullable: true; value: string };
      notes: { kind: "string"; nullable: true; value: string };
      rank: { kind: "float"; nullable: false; value: number };
    };
    relations: {
      activity: { many: "activity" };
      assignee: { one: "crew"; nullable: true };
      comments: { many: "comments" };
      show: { one: "shows"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:2ce75f9ae7ddb876ef6adaa4cdbe3e7c93aec8a0e7324140a32af6a9c1b8d4ed",
  "schema": {
    "current": "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
    "schemas": [
      {
        "hash": "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
        "schema": {
          "tables": {
            "activity": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "actorId",
                  "nullable": false,
                  "references": "crew",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["created", "moved", "assigned", "renamed", "commented", "deleted"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "detail", "nullable": true },
              ],
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
            "checklistItems": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
              ],
            },
            "comments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "authorId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
            "crew": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
            "showCrew": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "crewId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["chief", "crew"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "inviteCode", "nullable": true },
              ],
            },
            "showInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "shows": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "venue", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "date", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "doors", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "chiefAccount", "nullable": false },
              ],
            },
            "tasks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["todo", "doing", "done", "blocked"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assigneeId",
                  "nullable": true,
                  "references": "crew",
                },
                { "column_type": { "type": "Text" }, "name": "notes", "nullable": true },
                { "column_type": { "type": "Double" }, "name": "rank", "nullable": false },
              ],
            },
          },
        },
      },
      {
        "hash": "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
        "schema": {
          "tables": {
            "activity": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "actorId",
                  "nullable": false,
                  "references": "crew",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["created", "moved", "assigned", "renamed", "commented", "deleted"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "detail", "nullable": true },
              ],
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
            "checklistItems": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
              ],
            },
            "comments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "taskId",
                  "nullable": false,
                  "references": "tasks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "authorId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
            "crew": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
            "showCrew": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "crewId",
                  "nullable": false,
                  "references": "crew",
                },
                { "column_type": { "type": "Uuid" }, "name": "account", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["chief", "crew"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "inviteCode", "nullable": true },
              ],
            },
            "showInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "shows": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "venue", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "date", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "doors", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "chiefAccount", "nullable": false },
              ],
            },
            "tasks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "showId",
                  "nullable": false,
                  "references": "shows",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["todo", "doing", "done", "blocked"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assigneeId",
                  "nullable": true,
                  "references": "crew",
                },
                { "column_type": { "type": "Text" }, "name": "notes", "nullable": true },
                { "column_type": { "type": "Double" }, "name": "rank", "nullable": false },
              ],
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
        "fromHash": "c338db39787fc104e3d44141878b8c54365c95970079a08c3ddf24e17e00d499",
        "toHash": "f2c0deb18cb27616ed641ba2a2f620fb74ed606a7f21237863bb479533299ee7",
      },
    ],
  },
  "relations": {
    "activity": {
      "actor": { "one": "crew", "column": "actorId" },
      "show": { "one": "shows", "column": "showId" },
      "task": { "one": "tasks", "column": "taskId" },
    },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "checklistItems": {},
    "comments": {
      "author": { "one": "crew", "column": "authorId" },
      "task": { "one": "tasks", "column": "taskId" },
    },
    "crew": {
      "assignedTasks": { "many": "tasks", "relation": "assignee" },
      "memberships": { "many": "showCrew", "relation": "crew" },
    },
    "showCrew": {
      "crew": { "one": "crew", "column": "crewId" },
      "show": { "one": "shows", "column": "showId" },
    },
    "showInvites": { "show": { "one": "shows", "column": "showId" } },
    "shows": {
      "activity": { "many": "activity", "relation": "show" },
      "crew": { "many": "showCrew", "relation": "show" },
      "invites": { "many": "showInvites", "relation": "show" },
      "tasks": { "many": "tasks", "relation": "show" },
    },
    "tasks": {
      "activity": { "many": "activity", "relation": "task" },
      "assignee": { "one": "crew", "column": "assigneeId" },
      "comments": { "many": "comments", "relation": "task" },
      "show": { "one": "shows", "column": "showId" },
    },
  },
});
