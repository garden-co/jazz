import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";
import { schema as betterauthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterauthSchema,
  // Read by everyone whose `team` claim equals the row's team column.
  team_notes: s.table(
    {
      team: s.string(),
      author_name: s.string(),
      text: s.string(),
      sent_at: s.timestamp(),
    },
    {},
  ),
  // Owner-only rows.
  private_notes: s.table(
    {
      text: s.string(),
      sent_at: s.timestamp(),
    },
    {},
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations });
