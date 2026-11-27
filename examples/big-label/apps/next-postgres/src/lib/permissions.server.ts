// Who can do what in BigLabel (README "Roles", "Admission boundary"),
// enforced by the server. Roles: admins run the label, editors maintain the
// catalogue, viewers read it (src/roles.ts).
//
// - Every tenant row is readable by the label's members only.
// - A person is visible to themselves and to people who share a label.
// - Sign-in emails are never readable by clients.
// - Labels and first admins are created only by the server (bootstrap, demo data).
// - Members are added only by admins, by email (POST /api/members), and never
//   as admins; admins may change roles later.
// - Cross-tenant references are refused by composite foreign keys
//   (db/001_init.sql); the checks here are about roles.
import { one, type Db } from "./db.server";
import { catalogueEditors, invitableRoles, roles, type Role } from "../roles";

export class Forbidden extends Error {
  constructor(message = "permission_denied") {
    super(message);
  }
}

/** SQL: the labels the account in parameter `me` belongs to. */
export const myLabelsSql = (me: string) => `(SELECT "organizationId" FROM memberships WHERE "userId" = ${me})`;

export async function roleIn(db: Db, me: string, organizationId: string): Promise<Role | undefined> {
  const row = await one<{ role: Role }>(
    db,
    `SELECT role FROM memberships WHERE "organizationId" = $1 AND "userId" = $2`,
    [organizationId, me],
  );
  return row?.role;
}

async function assertRole(db: Db, me: string, organizationId: string, allowed: readonly Role[]) {
  const role = await roleIn(db, me, organizationId);
  if (!role || !allowed.includes(role)) throw new Forbidden();
}
export const assertMember = (db: Db, me: string, org: string) => assertRole(db, me, org, roles);
export const assertEditor = (db: Db, me: string, org: string) => assertRole(db, me, org, catalogueEditors);
export const assertAdmin = (db: Db, me: string, org: string) => assertRole(db, me, org, ["admin"]);

export const isInvitableRole = (role: string) => (invitableRoles as readonly string[]).includes(role);
export const isRole = (role: string) => (roles as readonly string[]).includes(role);

/** Who may subscribe to a live topic: a label's members to `org:<id>`. */
export async function canSubscribe(db: Db, me: string, topic: string) {
  const [kind, id] = topic.split(":");
  return kind === "org" && Boolean(id) && Boolean(await roleIn(db, me, id!));
}
