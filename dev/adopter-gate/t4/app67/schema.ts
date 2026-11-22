import { schema as s } from "jazz-tools";
import { tables as betterauthSchema } from "./schema-better-auth/schema";
import { permissions as betterauthPermissions } from "./schema-better-auth/schema";

export const schema = s.defineSchema({
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
});

const { definePermissions, session } = schema;

const createdByMe = { $createdBy: session.user };

const teamFromSession = { team: session.claims.team };

export const permissions = definePermissions({
  ...betterauthPermissions,
  private_notes: {
    read: createdByMe,
    insert: createdByMe,
    update: createdByMe,
    delete: createdByMe,
  },
  team_notes: {
    read: teamFromSession,
    insert: teamFromSession,
    update: false,
    delete: false,
  },
});
