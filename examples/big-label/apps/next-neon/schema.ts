/**
 * Row types of BigLabel's tables (db/001_init.sql), as the live queries
 * return them: columns aliased to the Jazz schema's camelCase field names,
 * relations included as JSON objects. Release dates are epoch milliseconds.
 */
export type Organization = { id: string; name: string; slug: string };
export type Person = { id: string; userId: string; name: string };
export type Membership = { id: string; organizationId: string; personId: string; userId: string; role: string };
export type Team = { id: string; organizationId: string; name: string };
export type TeamAssignment = { id: string; organizationId: string; teamId: string; membershipId: string; role: string };
export type Artist = { id: string; organizationId: string; name: string; genre: string; status: string; searchKey: string };
export type Catalogue = { id: string; organizationId: string; name: string; code: string };
export type Release = {
  id: string;
  organizationId: string;
  artistId: string;
  catalogueId: string | null;
  catalogNumber: string;
  catalogSequence: number | null;
  title: string;
  format: string;
  releaseDate: number;
  status: string;
  searchKey: string;
};
export type ReleaseTeam = { id: string; organizationId: string; releaseId: string; teamId: string };
