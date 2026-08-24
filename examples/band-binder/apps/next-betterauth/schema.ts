import { schema as s } from "jazz-tools";

// Workspace ids are intentionally denormalized onto every content row. That
// makes authorization scope explicit instead of inventing recursive permission
// inheritance through the page and block relations.
const schema = {
  workspaces: s.table({ name: s.string(), ownerSubject: s.string() }).indexOnly(["ownerSubject"]),
  members: s
    .table({
      workspaceId: s.ref("workspaces"),
      subject: s.string(),
      role: s.enum("owner", "member", "stage_manager"),
    })
    .indexOnly(["workspaceId", "subject", "role"]),
  pages: s
    .table({
      workspaceId: s.ref("workspaces"),
      parentPageId: s.ref("pages").optional(),
      title: s.string(),
    })
    .indexOnly(["workspaceId", "parentPageId", "title"]),
  blocks: s
    .table({
      workspaceId: s.ref("workspaces"),
      pageId: s.ref("pages"),
      parentBlockId: s.ref("blocks").optional(),
      position: s.float(),
      kind: s.enum("text", "song", "task", "calendar", "attachment", "page"),
      payload: s.json(),
    })
    .indexOnly(["workspaceId", "pageId", "position"]),
  tasks: s
    .table({
      workspaceId: s.ref("workspaces"),
      blockId: s.ref("blocks"),
      title: s.string(),
      completed: s.boolean(),
      assigneeSubject: s.string().optional(),
      dueAt: s.timestamp().optional(),
    })
    .indexOnly(["workspaceId", "dueAt", "blockId"]),
  calendarEvents: s
    .table({
      workspaceId: s.ref("workspaces"),
      blockId: s.ref("blocks"),
      title: s.string(),
      startsAt: s.timestamp(),
      endsAt: s.timestamp(),
    })
    .indexOnly(["workspaceId", "startsAt", "blockId"]),
  songs: s
    .table({
      workspaceId: s.ref("workspaces"),
      blockId: s.ref("blocks"),
      title: s.string(),
      key: s.string().optional(),
      bpm: s.float().optional(),
    })
    .indexOnly(["workspaceId", "title", "blockId"]),
  suggestions: s
    .table({
      workspaceId: s.ref("workspaces"),
      blockId: s.ref("blocks"),
      payload: s.json(),
      status: s.enum("open", "accepted", "rejected"),
    })
    .indexOnly(["workspaceId", "blockId", "status"]),
  attachments: s
    .table({
      workspaceId: s.ref("workspaces"),
      blockId: s.ref("blocks"),
      name: s.string(),
      mediaType: s.string(),
      bytes: s.bytes(),
    })
    .indexOnly(["workspaceId", "blockId", "name"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
