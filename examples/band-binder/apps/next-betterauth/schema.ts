import { schema as s } from "jazz-tools";

// BandBinder deliberately uses one recursive block relation for every page
// surface. A block can be prose, a song section, a task, a calendar entry, or
// an attachment placeholder; product rendering decides how to interpret it.
const schema = {
  workspaces: s.table({ name: s.string() }),
  members: s.table({
    workspace_id: s.ref("workspaces"),
    subject: s.string(),
    role: s.enum("owner", "member", "stage_manager"),
  }),
  pages: s.table({
    workspace_id: s.ref("workspaces"),
    parent_page_id: s.ref("pages").optional(),
    title: s.string(),
    branch: s.string(),
  }),
  blocks: s.table({
    page_id: s.ref("pages"),
    parent_block_id: s.ref("blocks").optional(),
    position: s.float(),
    kind: s.enum("text", "song", "task", "calendar", "attachment", "embed"),
    payload: s.json(),
  }),
  suggestions: s.table({
    block_id: s.ref("blocks"),
    branch: s.string(),
    payload: s.json(),
    status: s.enum("open", "accepted", "rejected"),
  }),
  attachments: s.table({
    page_id: s.ref("pages"),
    name: s.string(),
    media_type: s.string(),
    // Large-content streaming is intentionally tracked separately (#1833/#1839/#1844).
    content_locator: s.string().optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
