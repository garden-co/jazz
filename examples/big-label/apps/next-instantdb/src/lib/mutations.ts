import { db } from "./db";
import { formatCatalogNumber } from "../fixtures";

/**
 * Multi-row writes. Instant transactions are atomic but can't read, so the
 * checks the Jazz app makes in serializable transactions are made by the
 * server here: unique attributes and the rules in instant.perms.ts.
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
 * release's `catalogKey` ("<label>/<number>") is a unique attribute, so the
 * server refuses any write, through this function or not, that would give a
 * number to a second release.
 */
export async function saveRelease(
  organizationId: string,
  release: { id: string; isNew: boolean; artistId?: string; catalogueId?: string | null },
  { artistId, catalogueId, ...fields }: ReleaseFields,
) {
  const row = {
    ...fields,
    catalogSequence: catalogSequence(fields.catalogNumber),
    catalogKey: `${organizationId}/${fields.catalogNumber}`,
  };
  const chunk = release.isNew
    ? db.tx.releases[release.id].create(row).link({ organization: organizationId })
    : db.tx.releases[release.id].update(row);
  // A changed artist or catalogue: drop the old link in the same transaction.
  const unlinks = {
    ...(release.artistId && release.artistId !== artistId ? { artist: release.artistId } : {}),
    ...(release.catalogueId && release.catalogueId !== catalogueId
      ? { catalogue: release.catalogueId }
      : {}),
  };
  const relinks = Object.keys(unlinks).length > 0 ? chunk.unlink(unlinks) : chunk;
  try {
    await db.transact(
      relinks.link({ artist: artistId, ...(catalogueId ? { catalogue: catalogueId } : {}) }),
    );
  } catch (error) {
    if ((error as { body?: { type?: string } })?.body?.type === "record-not-unique")
      throw new CatalogNumberTakenError(fields.catalogNumber);
    throw error;
  }
}

/** The trailing digits of a catalogue number: 13 for "NFC-013". */
export function catalogSequence(catalogNumber: string) {
  const digits = catalogNumber.match(/(\d+)$/)?.[1];
  return digits === undefined ? null : Number(digits);
}

/** The number after the highest one used in a catalogue, e.g. NFC-013. */
export async function nextCatalogNumber(catalogue: { id: string; code: string }) {
  const [{ data: bySequence }, { data: byText }] = await Promise.all([
    db.queryOnce({
      releases: {
        $: {
          where: { catalogue: catalogue.id, catalogSequence: { $gte: 0 } },
          order: { catalogSequence: "desc" },
          limit: 1,
        },
      },
    }),
    // Releases saved before catalogSequence existed only have the text, which
    // sorts correctly as long as the numbers have the same width.
    db.queryOnce({
      releases: {
        $: { where: { catalogue: catalogue.id }, order: { catalogNumber: "desc" }, limit: 1 },
      },
    }),
  ]);
  const highest = bySequence.releases[0];
  const lastByText = byText.releases[0];
  const sequence = Math.max(
    highest?.catalogSequence ?? 0,
    (lastByText && catalogSequence(lastByText.catalogNumber)) ?? 0,
  );
  return formatCatalogNumber(catalogue.code, sequence + 1);
}

/**
 * Deletes an artist that has no releases. The rule in instant.perms.ts
 * refuses the delete while the artist has releases, so one added concurrently
 * still stops it.
 */
export function deleteArtist(artistId: string) {
  return db.transact(db.tx.artists[artistId].delete());
}

/** Deletes a release; its team and per-person assignments go with it (cascade). */
export function deleteRelease(releaseId: string) {
  return db.transact(db.tx.releases[releaseId].delete());
}

/** Deletes a team; its memberships and release assignments go with it (cascade). */
export function deleteTeam(teamId: string) {
  return db.transact(db.tx.teams[teamId].delete());
}

/** Removes a member from the label; their team and release assignments go with it (cascade). */
export function removeMember(membershipId: string) {
  return db.transact(db.tx.memberships[membershipId].delete());
}
