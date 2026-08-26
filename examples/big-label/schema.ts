import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  organizations: s.table({ name: s.string(), slug: s.string() }),
  people: s.table({ userId: s.string(), name: s.string() }),
  teams: s.table({ organizationId: s.ref("organizations"), name: s.string() }),
  memberships: s.table({
    organizationId: s.ref("organizations"),
    personId: s.ref("people"),
    userId: s.string(),
    role: s.string(),
  }),
  teamAssignments: s.table({
    organizationId: s.ref("organizations"),
    teamId: s.ref("teams"),
    membershipId: s.ref("memberships"),
    role: s.string(),
  }),
  artists: s.table({
    organizationId: s.ref("organizations"),
    name: s.string(),
    genre: s.string(),
    status: s.string(),
  }),
  releases: s.table({
    organizationId: s.ref("organizations"),
    artistId: s.ref("artists"),
    title: s.string(),
    releaseDate: s.timestamp(),
    status: s.string(),
  }),
  releaseAssignments: s.table({
    organizationId: s.ref("organizations"),
    releaseId: s.ref("releases"),
    membershipId: s.ref("memberships"),
    role: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
