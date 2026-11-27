"use client";

import { mutate, type Overlay } from "./live";
import { searchKey } from "../fixtures";
import type { Release } from "../../schema";

/**
 * BigLabel's writes, as the screens call them. Each names a server write
 * handler (mutators.server.ts, where the checks are), and says how it
 * changes the live queries at once, until its transaction has synced
 * (Neon's optimistic-mutations pattern). Multi-row writes (deleting a
 * release, team or member with its assignments; the catalogue-number check)
 * happen in the handler's transaction.
 */
type Rows = Parameters<Overlay[string]>[0];
type Row = Rows[number];
type Ref = { id: string; name: string };

const has = (rows: Rows, id: string) => rows.some((row) => row.id === id);
const without = (match: (row: Row) => boolean) => (rows: Rows) => (rows.some(match) ? rows.filter((row) => !match(row)) : rows);
const patch = (id: string, change: Record<string, unknown>) => (rows: Rows) =>
  has(rows, id) ? rows.map((row) => (row.id === id ? { ...row, ...change } : row)) : rows;
/** Applies `change` only to the queries whose arguments match. */
const where =
  (match: Record<string, unknown>, change: (rows: Rows) => Rows) =>
  (rows: Rows, args: Record<string, unknown>) =>
    Object.entries(match).every(([key, value]) => args[key] === value) ? change(rows) : rows;
const added = (row: Row) => (rows: Rows) => (has(rows, row.id as string) ? rows : [...rows, row]);
const byName = (a: Row, b: Row) => String(a.name).localeCompare(String(b.name));

export type ReleaseFields = {
  title: string;
  artistId: string;
  catalogueId: string | null;
  catalogNumber: string;
  format: string;
  releaseDate: number;
  status: string;
};

export const mutations = {
  renameLabel: (id: string, name: string) =>
    mutate("organizations.rename", { id, name }, {
      "memberships.mine": (rows) =>
        rows.map((row) => ((row.organization as Ref | null)?.id === id ? { ...row, organization: { id, name } } : row)),
    }),

  createArtist: (organizationId: string, fields: { name: string; genre: string; status: string }) => {
    const id = crypto.randomUUID();
    const artist = { id, organizationId, ...fields, searchKey: searchKey(fields.name, fields.genre) };
    return {
      id,
      write: mutate("artists.create", { id, organizationId, ...fields }, {
        "artists.one": where({ id }, (rows) => (rows.length ? rows : [artist])),
        "artists.options": where({ organizationId }, (rows) => added(artist)(rows).slice().sort(byName)),
      }),
    };
  },
  updateArtist: (id: string, fields: { name: string; genre: string; status: string }) => {
    const change = { ...fields, searchKey: searchKey(fields.name, fields.genre) };
    return mutate("artists.update", { id, ...fields }, {
      "artists.one": patch(id, change),
      "artists.page": patch(id, change),
      "artists.options": patch(id, change),
      "releases.page": (rows) => rows.map((r) => ((r.artist as Ref | null)?.id === id ? { ...r, artist: { id, name: fields.name } } : r)),
      "releases.latest": (rows) => rows.map((r) => ((r.artist as Ref | null)?.id === id ? { ...r, artist: { id, name: fields.name } } : r)),
      "releases.one": (rows) => rows.map((r) => ((r.artist as Ref | null)?.id === id ? { ...r, artist: { id, name: fields.name } } : r)),
    });
  },
  deleteArtist: (id: string) =>
    mutate("artists.delete", { id }, {
      "artists.one": without((row) => row.id === id),
      "artists.page": without((row) => row.id === id),
      "artists.options": without((row) => row.id === id),
    }),

  /**
   * Inserts or updates a release (the release form). The handler refuses a
   * catalogue number that's already in use in the label.
   */
  saveRelease: (
    organizationId: string,
    release: { id: string; isNew: boolean },
    fields: ReleaseFields,
    refs: { artist: Ref | null; catalogue: Ref | null },
  ) => {
    const { id } = release;
    const change = { ...fields, searchKey: searchKey(fields.title, fields.catalogNumber) };
    const row: Release & typeof refs = {
      id,
      organizationId,
      catalogSequence: null,
      ...change,
      ...refs,
    };
    const withRefs = { ...change, artist: refs.artist };
    return mutate("releases.save", { ...release, organizationId, ...fields }, {
      "releases.one": where({ id }, (rows) => (rows.length ? patch(id, { ...change, ...refs })(rows) : release.isNew ? [row] : rows)),
      "releases.page": patch(id, withRefs),
      "releases.latest": patch(id, withRefs),
      "releases.byArtist": patch(id, change),
      "releaseTeams.byTeam": (rows) =>
        rows.map((rt) => ((rt.release as Row | null)?.id === id ? { ...rt, release: { ...(rt.release as Row), ...change } } : rt)),
    });
  },
  deleteRelease: (id: string) =>
    mutate("releases.delete", { id }, {
      "releases.one": without((row) => row.id === id),
      "releases.page": without((row) => row.id === id),
      "releases.latest": without((row) => row.id === id),
      "releases.byArtist": without((row) => row.id === id),
      "releaseTeams.byTeam": without((row) => row.releaseId === id),
    }),

  assignTeam: (organizationId: string, releaseId: string, team: Ref) => {
    const id = crypto.randomUUID();
    const row = { id, organizationId, releaseId, teamId: team.id, team };
    return mutate("releaseTeams.add", { id, organizationId, releaseId, teamId: team.id }, {
      "releaseTeams.byRelease": where({ releaseId }, added(row)),
      "releaseTeams.byLabel": added({ id, teamId: team.id }),
    });
  },
  unassignTeam: (id: string) =>
    mutate("releaseTeams.remove", { id }, {
      "releaseTeams.byRelease": without((row) => row.id === id),
      "releaseTeams.byTeam": without((row) => row.id === id),
      "releaseTeams.byLabel": without((row) => row.id === id),
    }),

  createTeam: (organizationId: string, name: string) => {
    const id = crypto.randomUUID();
    const team = { id, organizationId, name };
    return {
      id,
      write: mutate("teams.create", { id, organizationId, name }, {
        "teams.one": where({ id }, (rows) => (rows.length ? rows : [team])),
        "teams.all": where({ organizationId }, (rows) => added(team)(rows).slice().sort(byName)),
      }),
    };
  },
  renameTeam: (id: string, name: string) =>
    mutate("teams.rename", { id, name }, {
      "teams.one": patch(id, { name }),
      "teams.all": (rows) => patch(id, { name })(rows).slice().sort(byName),
      "releaseTeams.byRelease": (rows) => rows.map((rt) => (rt.teamId === id ? { ...rt, team: { id, name } } : rt)),
    }),
  deleteTeam: (id: string) =>
    mutate("teams.delete", { id }, {
      "teams.one": without((row) => row.id === id),
      "teams.all": without((row) => row.id === id),
      "releaseTeams.byRelease": without((row) => row.teamId === id),
    }),

  addToTeam: (organizationId: string, teamId: string, membership: { id: string; name: string }, role: string) => {
    const id = crypto.randomUUID();
    const row = { id, organizationId, teamId, membershipId: membership.id, role, membership: { person: { name: membership.name } } };
    return mutate("teamAssignments.add", { id, organizationId, teamId, membershipId: membership.id, role }, {
      "teamAssignments.byTeam": where({ teamId }, added(row)),
      "teamAssignments.byLabel": added({ id, teamId }),
    });
  },
  removeFromTeam: (id: string) =>
    mutate("teamAssignments.remove", { id }, {
      "teamAssignments.byTeam": without((row) => row.id === id),
      "teamAssignments.byLabel": without((row) => row.id === id),
    }),

  createCatalogue: (organizationId: string, fields: { name: string; code: string }) => {
    const id = crypto.randomUUID();
    const catalogue = { id, organizationId, ...fields };
    return mutate("catalogues.create", { id, organizationId, ...fields }, {
      "catalogues.all": where({ organizationId }, (rows) => added(catalogue)(rows).slice().sort(byName)),
    });
  },
  updateCatalogue: (id: string, fields: { name: string; code: string }) =>
    mutate("catalogues.update", { id, ...fields }, {
      "catalogues.one": patch(id, fields),
      "catalogues.all": (rows) => patch(id, fields)(rows).slice().sort(byName),
    }),

  changeRole: (id: string, role: string) =>
    mutate("memberships.setRole", { id, role }, {
      "memberships.byLabel": patch(id, { role }),
      "memberships.mine": patch(id, { role }),
    }),
  /** Removes a member from the label, with their team and release assignments. */
  removeMember: (id: string) =>
    mutate("memberships.remove", { id }, {
      "memberships.byLabel": without((row) => row.id === id),
      "teamAssignments.byTeam": without((row) => row.membershipId === id),
    }),
};
