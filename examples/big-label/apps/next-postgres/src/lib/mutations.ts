import { call } from "../data/api";
import { type Db } from "../data/react";
import { app } from "../data/query";
import { formatCatalogNumber } from "../fixtures";

/**
 * Multi-row writes. Each is one server call, a transaction that reads the
 * rows it touches, so it acts on the database as it is, not on whatever page
 * of rows the UI happened to have loaded.
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

const sent = (promise: Promise<unknown>) => {
  const done = promise.then(() => undefined);
  done.catch(() => {}); // reported by whoever waits
  return { wait: (_options?: unknown) => done };
};

/**
 * Inserts or updates a release. Catalogue numbers are unique per label: the
 * server's serializable transaction reads the number's current holders, and
 * a UNIQUE constraint backs it up.
 */
export async function saveRelease(
  _db: Db,
  organizationId: string,
  release: { id: string; isNew: boolean },
  fields: ReleaseFields,
) {
  const row = { ...fields, catalogSequence: catalogSequence(fields.catalogNumber) };
  return sent(call("saveRelease", { organizationId, id: release.id, isNew: release.isNew, fields: row }));
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

/** Deletes an artist that has no releases (the foreign key refuses otherwise). */
export async function deleteArtist(db: Db, _organizationId: string, artistId: string) {
  return db.delete(app.artists, artistId);
}

/** Deletes a release; its team and per-person assignments go with it (ON DELETE CASCADE). */
export async function deleteRelease(db: Db, _organizationId: string, releaseId: string) {
  return db.delete(app.releases, releaseId);
}

/** Deletes a team with its memberships and release assignments (cascade). */
export async function deleteTeam(db: Db, _organizationId: string, teamId: string) {
  return db.delete(app.teams, teamId);
}

/** Removes a member from the label, with their team and release assignments (cascade). */
export async function removeMember(db: Db, _organizationId: string, membershipId: string) {
  return db.delete(app.memberships, membershipId);
}
