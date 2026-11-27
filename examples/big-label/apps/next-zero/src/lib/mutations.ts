import type { Zero } from "@rocicorp/zero";
import { catalogSequence, mutators } from "../../mutators";
import { queries } from "../../queries";
import { formatCatalogNumber } from "../fixtures";
import { confirmed } from "./write-errors";

/**
 * Multi-row writes. Each is one mutator: it runs at once on the client and
 * again in a Postgres transaction on the server, which reads the rows it
 * touches there, so it acts on the database as it is. Each resolves once the
 * server applied it and rejects if the server refused.
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

/**
 * Inserts or updates a release. Catalogue numbers are unique per label: the
 * server's transaction reads the number's current holders, and a unique index
 * refuses a concurrent duplicate.
 */
export function saveRelease(
  db: Zero,
  organizationId: string,
  release: { id: string; isNew: boolean },
  fields: ReleaseFields,
) {
  return confirmed(
    db.mutate(
      mutators.releases.save({
        ...fields,
        releaseDate: fields.releaseDate.getTime(),
        id: release.id,
        isNew: release.isNew,
        organizationId,
      }),
    ),
  );
}

export { catalogSequence };

/** The number after the highest one used in a catalogue, e.g. NFC-013. */
export async function nextCatalogNumber(
  db: Zero,
  organizationId: string,
  catalogue: { id: string; code: string },
) {
  const inCatalogue = { organizationId, catalogueId: catalogue.id };
  const [[highest], [lastByText]] = await Promise.all([
    db.run(queries.releases.lastInCatalogue({ ...inCatalogue, by: "catalogSequence" }), { type: "complete" }),
    // Releases saved before catalogSequence existed only have the text, which
    // sorts correctly as long as the numbers have the same width.
    db.run(queries.releases.lastInCatalogue({ ...inCatalogue, by: "catalogNumber" }), { type: "complete" }),
  ]);
  const sequence = Math.max(
    highest?.catalogSequence ?? 0,
    (lastByText && catalogSequence(lastByText.catalogNumber)) ?? 0,
  );
  return formatCatalogNumber(catalogue.code, sequence + 1);
}

/** Deletes an artist that has no releases (the server checks, and Postgres restricts it). */
export function deleteArtist(db: Zero, organizationId: string, artistId: string) {
  return confirmed(db.mutate(mutators.artists.delete({ id: artistId, organizationId })));
}

/** Deletes a release with its team and per-person assignments. */
export function deleteRelease(db: Zero, organizationId: string, releaseId: string) {
  return confirmed(db.mutate(mutators.releases.delete({ id: releaseId, organizationId })));
}

/** Deletes a team with its memberships and release assignments. */
export function deleteTeam(db: Zero, organizationId: string, teamId: string) {
  return confirmed(db.mutate(mutators.teams.delete({ id: teamId, organizationId })));
}

/** Removes a member from the label, with their team and release assignments. */
export function removeMember(db: Zero, organizationId: string, membershipId: string) {
  return confirmed(db.mutate(mutators.memberships.remove({ id: membershipId, organizationId })));
}
