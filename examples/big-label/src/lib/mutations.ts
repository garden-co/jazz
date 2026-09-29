import type { Db } from "jazz-tools";
import { app } from "../../schema";
import { formatCatalogNumber } from "../fixtures";

/**
 * Multi-row writes. Each one reads the rows it touches inside its own
 * transaction, so it acts on the database as it is, not on whatever page of
 * rows the UI happened to have loaded.
 */

export type ReleaseFields = {
  title: string;
  artistId: string;
  catalogueId: string | null;
  catalogNumber: string;
  format: string;
  releaseDate: Date;
  status: string;
  searchKey: string;
};

export class CatalogNumberTakenError extends Error {
  constructor(readonly catalogNumber: string) {
    super(`${catalogNumber} is already in use`);
  }
}

/**
 * Inserts or updates a release. Catalogue numbers are unique per label: the
 * exclusive transaction reads the number's current holders, and the
 * authority rejects the write if a concurrent transaction changed that read.
 * This protects writes made through this function; the permissions don't
 * enforce uniqueness on their own.
 */
export function saveRelease(
  db: Db,
  organizationId: string,
  release: { id: string; isNew: boolean },
  fields: ReleaseFields,
) {
  return db.exclusiveTransaction(async (tx) => {
    const holders = await tx.all(
      app.releases.where({ organizationId, catalogNumber: fields.catalogNumber }),
    );
    if (holders.some((row) => row.id !== release.id))
      throw new CatalogNumberTakenError(fields.catalogNumber);
    const row = { ...fields, catalogSequence: catalogSequence(fields.catalogNumber) };
    if (release.isNew) tx.insert(app.releases, { ...row, organizationId }, { id: release.id });
    else tx.update(app.releases, release.id, row);
  });
}

/** The trailing digits of a catalogue number: 13 for "NFC-013". */
export function catalogSequence(catalogNumber: string) {
  const digits = catalogNumber.match(/(\d+)$/)?.[1];
  return digits === undefined ? null : Number(digits);
}

/** The number after the highest one used in a catalogue, e.g. NFC-013. */
export async function nextCatalogNumber(
  db: Db,
  organizationId: string,
  catalogue: { id: string; code: string },
) {
  const inCatalogue = { organizationId, catalogueId: catalogue.id };
  const [[highest], [lastByText]] = await Promise.all([
    db.all(
      app.releases
        .where({ ...inCatalogue, catalogSequence: { gte: 0 } })
        .orderBy("catalogSequence", "desc")
        .limit(1),
    ),
    // Releases saved before catalogSequence existed only have the text, which
    // sorts correctly as long as the numbers have the same width.
    db.all(app.releases.where(inCatalogue).orderBy("catalogNumber", "desc").limit(1)),
  ]);
  const sequence = Math.max(
    highest?.catalogSequence ?? 0,
    (lastByText && catalogSequence(lastByText.catalogNumber)) ?? 0,
  );
  return formatCatalogNumber(catalogue.code, sequence + 1);
}

export class ArtistHasReleasesError extends Error {
  constructor() {
    super("Delete this artist's releases first");
  }
}

/**
 * Deletes an artist that has no releases. Jazz has no foreign-key restrict
 * yet, so the exclusive transaction reads the artist's releases and the
 * authority rejects the delete if one was added concurrently.
 */
export function deleteArtist(db: Db, organizationId: string, artistId: string) {
  return db.exclusiveTransaction(async (tx) => {
    const releases = await tx.all(app.releases.where({ organizationId, artistId }).limit(1));
    if (releases.length > 0) throw new ArtistHasReleasesError();
    tx.delete(app.artists, artistId);
  });
}

/** Deletes a release with its team and per-person assignments. */
export function deleteRelease(db: Db, organizationId: string, releaseId: string) {
  return db.transaction(async (tx) => {
    const [teams, people] = await Promise.all([
      tx.all(app.releaseTeams.where({ organizationId, releaseId })),
      tx.all(app.releaseAssignments.where({ organizationId, releaseId })),
    ]);
    for (const row of teams) tx.delete(app.releaseTeams, row.id);
    for (const row of people) tx.delete(app.releaseAssignments, row.id);
    tx.delete(app.releases, releaseId);
  });
}

/** Deletes a team with its memberships and release assignments. */
export function deleteTeam(db: Db, organizationId: string, teamId: string) {
  return db.transaction(async (tx) => {
    const [members, releases] = await Promise.all([
      tx.all(app.teamAssignments.where({ organizationId, teamId })),
      tx.all(app.releaseTeams.where({ organizationId, teamId })),
    ]);
    for (const row of members) tx.delete(app.teamAssignments, row.id);
    for (const row of releases) tx.delete(app.releaseTeams, row.id);
    tx.delete(app.teams, teamId);
  });
}

/** Removes a member from the label, with their team and release assignments. */
export function removeMember(db: Db, organizationId: string, membershipId: string) {
  return db.transaction(async (tx) => {
    const [teams, releases] = await Promise.all([
      tx.all(app.teamAssignments.where({ organizationId, membershipId })),
      tx.all(app.releaseAssignments.where({ organizationId, membershipId })),
    ]);
    for (const row of teams) tx.delete(app.teamAssignments, row.id);
    for (const row of releases) tx.delete(app.releaseAssignments, row.id);
    tx.delete(app.memberships, membershipId);
  });
}
