import { schema as s } from "jazz-tools";

const schema = {
  // Better Auth's persistence belongs to this example rather than a shared
  // example helper: it is part of the runnable, copyable integration. Its
  // required timestamp names describe Better Auth records, not Jazz provenance.
  better_auth_user: s.table({
    name: s.string(),
    email: s.string(),
    emailVerified: s.boolean(),
    image: s.string().optional(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_session: s.table({
    expiresAt: s.timestamp(),
    token: s.string(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
    userId: s.ref("better_auth_user"),
    ipAddress: s.string().optional(),
    userAgent: s.string().optional(),
  }),
  better_auth_account: s.table({
    accountId: s.string(),
    providerId: s.string(),
    userId: s.ref("better_auth_user"),
    password: s.string().optional(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_verification: s.table({
    identifier: s.string(),
    value: s.string(),
    expiresAt: s.timestamp(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_jwks: s.table({
    publicKey: s.string(),
    privateKey: s.string(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    expiresAt: s.timestamp().optional(),
  }),
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
