import { defineMutator, defineMutators, type Transaction } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData } from "./schema";
import { catalogueEditors, invitableRoles, roles, type Role } from "./src/roles";

/**
 * Who can do what in BigLabel (reads are filtered in queries.ts). Roles (see
 * src/roles.ts): admins run the label, editors maintain the catalogue,
 * viewers read it.
 *
 * - Labels, people and first admins come only from the server (bootstrap and
 *   demo-data routes): no mutator creates them.
 * - Admins add members (by email, through POST /api/members, which runs
 *   `memberships.add` as the caller), never as admins; they change roles and
 *   remove members, manage teams and catalogues, delete records, rename the
 *   label and staff people on releases.
 * - Editors add and edit artists and releases and assign releases to teams.
 * - Everything a row points at (artist, catalogue, team, membership, release)
 *   must be in the row's own label.
 * - Catalogue numbers are unique per label: `releases.save` checks the
 *   number's holders in its transaction, and a unique index backs it up.
 *
 * Mutators run optimistically on the client and again, authoritatively, in a
 * Postgres transaction on the server, where these checks run.
 */
function mustUser(ctx: AuthData | undefined) {
  if (!ctx?.userID) throw new Error("permission_denied: not signed in");
  return ctx.userID;
}
const onServer = (tx: Transaction) => tx.location === "server";

async function assertRole(tx: Transaction, ctx: AuthData | undefined, organizationId: string, allowed: readonly Role[]) {
  const me = mustUser(ctx);
  if (!onServer(tx)) return;
  const membership = await tx.run(
    zql.memberships.where("organizationId", organizationId).where("userId", me).one(),
  );
  if (!membership || !allowed.includes(membership.role as Role))
    throw new Error("permission_denied: your role can't do that");
}
const assertAdmin = (tx: Transaction, ctx: AuthData | undefined, organizationId: string) =>
  assertRole(tx, ctx, organizationId, ["admin"]);
const assertEditor = (tx: Transaction, ctx: AuthData | undefined, organizationId: string) =>
  assertRole(tx, ctx, organizationId, catalogueEditors);

type TenantTable = "teams" | "memberships" | "artists" | "catalogues" | "releases";
/** The row exists and belongs to the label. Returns it (on the client: as far as synced). */
async function inLabel(tx: Transaction, table: TenantTable, id: string, organizationId: string) {
  const q =
    table === "teams" ? zql.teams
    : table === "memberships" ? zql.memberships
    : table === "artists" ? zql.artists
    : table === "catalogues" ? zql.catalogues
    : zql.releases;
  const row = await tx.run((q as typeof zql.teams).where("id", id).one());
  if (onServer(tx) && row?.organizationId !== organizationId)
    throw new Error(`permission_denied: that ${table.slice(0, -1)} is not in this label`);
  return row;
}

/** The trailing digits of a catalogue number: 13 for "NFC-013". */
export function catalogSequence(catalogNumber: string) {
  const digits = catalogNumber.match(/(\d+)$/)?.[1];
  return digits === undefined ? null : Number(digits);
}

const id = z.string();
const label = { organizationId: id };
const releaseFields = z.object({
  title: z.string().min(1),
  artistId: id,
  catalogueId: id.nullable(),
  catalogNumber: z.string().min(1),
  format: z.string(),
  releaseDate: z.number(),
  status: z.string(),
  searchKey: z.string(),
});
const artistFields = z.object({ name: z.string().min(1), genre: z.string(), status: z.string(), searchKey: z.string() });

export const mutators = defineMutators({
  organizations: {
    rename: defineMutator(z.object({ id, name: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.id);
      await tx.mutate.organizations.update(args);
    }),
  },

  memberships: {
    /**
     * Adds a person to a label (POST /api/members runs this as the caller):
     * only admins, only as editors or viewers, and the userId must be the
     * person's. Adding someone twice adds them once.
     */
    add: defineMutator(
      z.object({ id, ...label, personId: id, userId: id, role: z.string() }),
      async ({ tx, ctx, args }) => {
        await assertAdmin(tx, ctx, args.organizationId);
        if (!invitableRoles.includes(args.role as Role)) throw new Error("permission_denied: new members can't be admins");
        if (onServer(tx)) {
          const person = await tx.run(zql.people.where("id", args.personId).one());
          if (person?.userId !== args.userId) throw new Error("permission_denied: that isn't this person's account");
        }
        const existing = await tx.run(
          zql.memberships.where("organizationId", args.organizationId).where("personId", args.personId).one(),
        );
        if (existing) return { status: "already-member" };
        await tx.mutate.memberships.insert(args);
        return { status: "added" };
      },
    ),
    setRole: defineMutator(z.object({ id, ...label, role: z.enum(roles) }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      await inLabel(tx, "memberships", args.id, args.organizationId);
      await tx.mutate.memberships.update({ id: args.id, role: args.role });
    }),
    /** Removes a member from the label, with their team and release assignments. */
    remove: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      await inLabel(tx, "memberships", args.id, args.organizationId);
      for (const row of await tx.run(zql.teamAssignments.where("membershipId", args.id)))
        await tx.mutate.teamAssignments.delete({ id: row.id });
      for (const row of await tx.run(zql.releaseAssignments.where("membershipId", args.id)))
        await tx.mutate.releaseAssignments.delete({ id: row.id });
      await tx.mutate.memberships.delete({ id: args.id });
    }),
  },

  teams: {
    save: defineMutator(z.object({ id, ...label, name: z.string().min(1), isNew: z.boolean() }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      if (args.isNew) {
        if (onServer(tx) && (await tx.run(zql.teams.where("id", args.id).one()))) throw new Error("That id is taken");
        await tx.mutate.teams.insert({ id: args.id, organizationId: args.organizationId, name: args.name });
      } else {
        await inLabel(tx, "teams", args.id, args.organizationId);
        await tx.mutate.teams.update({ id: args.id, name: args.name });
      }
    }),
    /** Deletes a team with its memberships and release assignments. */
    delete: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      await inLabel(tx, "teams", args.id, args.organizationId);
      for (const row of await tx.run(zql.teamAssignments.where("teamId", args.id)))
        await tx.mutate.teamAssignments.delete({ id: row.id });
      for (const row of await tx.run(zql.releaseTeams.where("teamId", args.id)))
        await tx.mutate.releaseTeams.delete({ id: row.id });
      await tx.mutate.teams.delete({ id: args.id });
    }),
  },

  teamAssignments: {
    add: defineMutator(
      z.object({ id, ...label, teamId: id, membershipId: id, role: z.string() }),
      async ({ tx, ctx, args }) => {
        await assertAdmin(tx, ctx, args.organizationId);
        await inLabel(tx, "teams", args.teamId, args.organizationId);
        await inLabel(tx, "memberships", args.membershipId, args.organizationId);
        await tx.mutate.teamAssignments.insert(args);
      },
    ),
    remove: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      const row = await tx.run(zql.teamAssignments.where("id", args.id).one());
      if (onServer(tx) && row?.organizationId !== args.organizationId) throw new Error("permission_denied: not in this label");
      await tx.mutate.teamAssignments.delete({ id: args.id });
    }),
  },

  artists: {
    save: defineMutator(
      artistFields.extend({ id, ...label, isNew: z.boolean() }),
      async ({ tx, ctx, args: { isNew, ...artist } }) => {
        await assertEditor(tx, ctx, artist.organizationId);
        if (isNew) {
          if (onServer(tx) && (await tx.run(zql.artists.where("id", artist.id).one()))) throw new Error("That id is taken");
          await tx.mutate.artists.insert(artist);
        } else {
          await inLabel(tx, "artists", artist.id, artist.organizationId);
          const { organizationId: _, ...fields } = artist;
          await tx.mutate.artists.update(fields);
        }
      },
    ),
    /** Deletes an artist that has no releases (Postgres also restricts it). */
    delete: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      await inLabel(tx, "artists", args.id, args.organizationId);
      const releases = await tx.run(zql.releases.where("artistId", args.id).limit(1));
      if (releases.length > 0) throw new Error("Delete this artist's releases first");
      await tx.mutate.artists.delete({ id: args.id });
    }),
  },

  catalogues: {
    save: defineMutator(
      z.object({ id, ...label, name: z.string().min(1), code: z.string().min(1), isNew: z.boolean() }),
      async ({ tx, ctx, args: { isNew, ...catalogue } }) => {
        await assertAdmin(tx, ctx, catalogue.organizationId);
        if (isNew) {
          if (onServer(tx) && (await tx.run(zql.catalogues.where("id", catalogue.id).one()))) throw new Error("That id is taken");
          await tx.mutate.catalogues.insert(catalogue);
        } else {
          await inLabel(tx, "catalogues", catalogue.id, catalogue.organizationId);
          await tx.mutate.catalogues.update({ id: catalogue.id, name: catalogue.name, code: catalogue.code });
        }
      },
    ),
  },

  releases: {
    /**
     * Inserts or updates a release. Catalogue numbers are unique per label:
     * the transaction reads the number's current holders (and the unique
     * index on (organizationId, catalogNumber) refuses a concurrent duplicate).
     */
    save: defineMutator(
      releaseFields.extend({ id, ...label, isNew: z.boolean() }),
      async ({ tx, ctx, args: { isNew, ...release } }) => {
        await assertEditor(tx, ctx, release.organizationId);
        await inLabel(tx, "artists", release.artistId, release.organizationId);
        if (release.catalogueId) await inLabel(tx, "catalogues", release.catalogueId, release.organizationId);
        const holders = await tx.run(
          zql.releases.where("organizationId", release.organizationId).where("catalogNumber", release.catalogNumber),
        );
        if (holders.some((row) => row.id !== release.id)) throw new Error(`${release.catalogNumber} is already in use`);
        const row = { ...release, catalogSequence: catalogSequence(release.catalogNumber) };
        if (isNew) {
          if (onServer(tx) && (await tx.run(zql.releases.where("id", release.id).one()))) throw new Error("That id is taken");
          await tx.mutate.releases.insert(row);
        } else {
          await inLabel(tx, "releases", release.id, release.organizationId);
          const { organizationId: _, ...fields } = row;
          await tx.mutate.releases.update(fields);
        }
      },
    ),
    /** Deletes a release with its team and per-person assignments. */
    delete: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertAdmin(tx, ctx, args.organizationId);
      await inLabel(tx, "releases", args.id, args.organizationId);
      for (const row of await tx.run(zql.releaseTeams.where("releaseId", args.id)))
        await tx.mutate.releaseTeams.delete({ id: row.id });
      for (const row of await tx.run(zql.releaseAssignments.where("releaseId", args.id)))
        await tx.mutate.releaseAssignments.delete({ id: row.id });
      await tx.mutate.releases.delete({ id: args.id });
    }),
  },

  releaseTeams: {
    /** Editors staff releases with teams; both must be in the label. */
    add: defineMutator(z.object({ id, ...label, releaseId: id, teamId: id }), async ({ tx, ctx, args }) => {
      await assertEditor(tx, ctx, args.organizationId);
      await inLabel(tx, "releases", args.releaseId, args.organizationId);
      await inLabel(tx, "teams", args.teamId, args.organizationId);
      await tx.mutate.releaseTeams.insert(args);
    }),
    remove: defineMutator(z.object({ id, ...label }), async ({ tx, ctx, args }) => {
      await assertEditor(tx, ctx, args.organizationId);
      const row = await tx.run(zql.releaseTeams.where("id", args.id).one());
      if (onServer(tx) && row?.organizationId !== args.organizationId) throw new Error("permission_denied: not in this label");
      await tx.mutate.releaseTeams.delete({ id: args.id });
    }),
  },
});
