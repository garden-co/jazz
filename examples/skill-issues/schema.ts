import { schema as s } from "jazz-tools";

const schema = {
  users: s.table({
    jazzUserId: s.string(),
    githubUserId: s.string(),
    githubLogin: s.string(),
    verifiedAt: s.string(),
  }),
  items: s.table({
    kind: s.enum("idea", "issue"),
    title: s.string(),
    description: s.string(),
    slug: s.string(),
  }),
  itemStates: s.table({
    itemSlug: s.string(),
    status: s.enum("open", "in_progress", "done"),
    assigneeUserId: s.ref("users").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
