// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 3a1a3fe9a935ce96. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Artist = {
  readonly id: string;
  organizationId: string;
  name: string;
  genre: string;
  status: string;
  searchKey: string;
};

export type ArtistInsert = {
  organizationId: string;
  name: string;
  genre: string;
  status: string;
  searchKey: string;
};

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  accessToken: string | null;
  refreshToken: string | null;
  idToken: string | null;
  accessTokenExpiresAt: Date | null;
  refreshTokenExpiresAt: Date | null;
  scope: string | null;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  accessToken?: string | null;
  refreshToken?: string | null;
  idToken?: string | null;
  accessTokenExpiresAt?: Date | null;
  refreshTokenExpiresAt?: Date | null;
  scope?: string | null;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress: string | null;
  userAgent: string | null;
  userId: string;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string | null;
  userAgent?: string | null;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type Catalogue = {
  readonly id: string;
  organizationId: string;
  name: string;
  code: string;
};

export type CatalogueInsert = {
  organizationId: string;
  name: string;
  code: string;
};

export type Membership = {
  readonly id: string;
  organizationId: string;
  personId: string;
  userId: string;
  role: string;
};

export type MembershipInsert = {
  organizationId: string;
  personId: string;
  userId: string;
  role: string;
};

export type Organization = {
  readonly id: string;
  name: string;
  slug: string;
};

export type OrganizationInsert = {
  name: string;
  slug: string;
};

export type Person = {
  readonly id: string;
  userId: string;
  name: string;
};

export type PersonInsert = {
  userId: string;
  name: string;
};

export type PersonEmail = {
  readonly id: string;
  personId: string;
  email: string;
};

export type PersonEmailInsert = {
  personId: string;
  email: string;
};

export type ReleaseAssignment = {
  readonly id: string;
  organizationId: string;
  releaseId: string;
  membershipId: string;
  role: string;
};

export type ReleaseAssignmentInsert = {
  organizationId: string;
  releaseId: string;
  membershipId: string;
  role: string;
};

export type ReleaseTeam = {
  readonly id: string;
  organizationId: string;
  releaseId: string;
  teamId: string;
};

export type ReleaseTeamInsert = {
  organizationId: string;
  releaseId: string;
  teamId: string;
};

export type Release = {
  readonly id: string;
  organizationId: string;
  artistId: string;
  catalogueId: string | null;
  catalogNumber: string;
  catalogSequence: number | null;
  title: string;
  format: string;
  releaseDate: Date;
  status: string;
  searchKey: string;
};

export type ReleaseInsert = {
  organizationId: string;
  artistId: string;
  catalogueId?: string | null;
  catalogNumber: string;
  catalogSequence?: number | null;
  title: string;
  format: string;
  releaseDate: Date;
  status: string;
  searchKey: string;
};

export type TeamAssignment = {
  readonly id: string;
  organizationId: string;
  teamId: string;
  membershipId: string;
  role: string;
};

export type TeamAssignmentInsert = {
  organizationId: string;
  teamId: string;
  membershipId: string;
  role: string;
};

export type Team = {
  readonly id: string;
  organizationId: string;
  name: string;
};

export type TeamInsert = {
  organizationId: string;
  name: string;
};

interface Types extends AppTypes {
  artists: {
    row: Artist;
    insert: ArtistInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      genre: { kind: "string"; nullable: false; value: string };
      status: { kind: "string"; nullable: false; value: string };
      searchKey: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      releasesViaArtist: { many: "releases" };
    };
  };
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      accessToken: { kind: "string"; nullable: true; value: string };
      refreshToken: { kind: "string"; nullable: true; value: string };
      idToken: { kind: "string"; nullable: true; value: string };
      accessTokenExpiresAt: { kind: "timestamp"; nullable: true; value: Date };
      refreshTokenExpiresAt: { kind: "timestamp"; nullable: true; value: Date };
      scope: { kind: "string"; nullable: true; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  catalogues: {
    row: Catalogue;
    insert: CatalogueInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      releasesViaCatalogue: { many: "releases" };
    };
  };
  memberships: {
    row: Membership;
    insert: MembershipInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      personId: { kind: "uuid"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      person: { one: "people"; nullable: false };
      releaseAssignmentsViaMembership: { many: "releaseAssignments" };
      teamAssignmentsViaMembership: { many: "teamAssignments" };
    };
  };
  organizations: {
    row: Organization;
    insert: OrganizationInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      slug: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      artistsViaOrganization: { many: "artists" };
      cataloguesViaOrganization: { many: "catalogues" };
      membershipsViaOrganization: { many: "memberships" };
      releaseAssignmentsViaOrganization: { many: "releaseAssignments" };
      releaseTeamsViaOrganization: { many: "releaseTeams" };
      releasesViaOrganization: { many: "releases" };
      teamAssignmentsViaOrganization: { many: "teamAssignments" };
      teamsViaOrganization: { many: "teams" };
    };
  };
  people: {
    row: Person;
    insert: PersonInsert;
    columns: {
      userId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      membershipsViaPerson: { many: "memberships" };
      personEmailsViaPerson: { many: "personEmails" };
    };
  };
  personEmails: {
    row: PersonEmail;
    insert: PersonEmailInsert;
    columns: {
      personId: { kind: "uuid"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      person: { one: "people"; nullable: false };
    };
  };
  releaseAssignments: {
    row: ReleaseAssignment;
    insert: ReleaseAssignmentInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      releaseId: { kind: "uuid"; nullable: false; value: string };
      membershipId: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      membership: { one: "memberships"; nullable: false };
      organization: { one: "organizations"; nullable: false };
      release: { one: "releases"; nullable: false };
    };
  };
  releaseTeams: {
    row: ReleaseTeam;
    insert: ReleaseTeamInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      releaseId: { kind: "uuid"; nullable: false; value: string };
      teamId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      release: { one: "releases"; nullable: false };
      team: { one: "teams"; nullable: false };
    };
  };
  releases: {
    row: Release;
    insert: ReleaseInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      artistId: { kind: "uuid"; nullable: false; value: string };
      catalogueId: { kind: "uuid"; nullable: true; value: string };
      catalogNumber: { kind: "string"; nullable: false; value: string };
      catalogSequence: { kind: "int"; nullable: true; value: number };
      title: { kind: "string"; nullable: false; value: string };
      format: { kind: "string"; nullable: false; value: string };
      releaseDate: { kind: "timestamp"; nullable: false; value: Date };
      status: { kind: "string"; nullable: false; value: string };
      searchKey: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      artist: { one: "artists"; nullable: false };
      catalogue: { one: "catalogues"; nullable: true };
      organization: { one: "organizations"; nullable: false };
      releaseAssignmentsViaRelease: { many: "releaseAssignments" };
      releaseTeamsViaRelease: { many: "releaseTeams" };
    };
  };
  teamAssignments: {
    row: TeamAssignment;
    insert: TeamAssignmentInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      teamId: { kind: "uuid"; nullable: false; value: string };
      membershipId: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      membership: { one: "memberships"; nullable: false };
      organization: { one: "organizations"; nullable: false };
      team: { one: "teams"; nullable: false };
    };
  };
  teams: {
    row: Team;
    insert: TeamInsert;
    columns: {
      organizationId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      organization: { one: "organizations"; nullable: false };
      releaseTeamsViaTeam: { many: "releaseTeams" };
      teamAssignmentsViaTeam: { many: "teamAssignments" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:5368676a3dbb7716e11978cc0e528d3991dce4698044e4ad88bc3ba124ae16d8",
  "schema": {
    "current": "d986c626f864b1dd9e3e6a642819200d76f0e67d63da84553f95ceaa2e2ef207",
    "schemas": [
      {
        "hash": "b6640bee549149d8eaf9f9f8e7cddf486d6b3bd50cee2ddbb06a855e1980aa04",
        "schema": {
          "tables": {
            "artists": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "genre", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "searchKey", "nullable": false },
              ],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "issuer", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "accessToken", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "refreshToken", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "idToken", "nullable": true },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "accessTokenExpiresAt",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "refreshTokenExpiresAt",
                  "nullable": true,
                },
                { "column_type": { "type": "Text" }, "name": "scope", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "catalogues": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "memberships": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "personId",
                  "nullable": false,
                  "references": "people",
                },
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "organizations": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
              ],
            },
            "people": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
            "personEmails": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "personId",
                  "nullable": false,
                  "references": "people",
                },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
              ],
            },
            "releaseAssignments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "releaseId",
                  "nullable": false,
                  "references": "releases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "membershipId",
                  "nullable": false,
                  "references": "memberships",
                },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "releaseTeams": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "releaseId",
                  "nullable": false,
                  "references": "releases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "teamId",
                  "nullable": false,
                  "references": "teams",
                },
              ],
            },
            "releases": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "catalogueId",
                  "nullable": true,
                  "references": "catalogues",
                },
                { "column_type": { "type": "Text" }, "name": "catalogNumber", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "catalogSequence",
                  "nullable": true,
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "format", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "releaseDate", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "searchKey", "nullable": false },
              ],
            },
            "teamAssignments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "teamId",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "membershipId",
                  "nullable": false,
                  "references": "memberships",
                },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "teams": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
          },
        },
      },
      {
        "hash": "d986c626f864b1dd9e3e6a642819200d76f0e67d63da84553f95ceaa2e2ef207",
        "schema": {
          "tables": {
            "artists": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "genre", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "searchKey", "nullable": false },
              ],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "accessToken", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "refreshToken", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "idToken", "nullable": true },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "accessTokenExpiresAt",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "refreshTokenExpiresAt",
                  "nullable": true,
                },
                { "column_type": { "type": "Text" }, "name": "scope", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "catalogues": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "memberships": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "personId",
                  "nullable": false,
                  "references": "people",
                },
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "organizations": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "slug", "nullable": false },
              ],
            },
            "people": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
            "personEmails": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "personId",
                  "nullable": false,
                  "references": "people",
                },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
              ],
            },
            "releaseAssignments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "releaseId",
                  "nullable": false,
                  "references": "releases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "membershipId",
                  "nullable": false,
                  "references": "memberships",
                },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "releaseTeams": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "releaseId",
                  "nullable": false,
                  "references": "releases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "teamId",
                  "nullable": false,
                  "references": "teams",
                },
              ],
            },
            "releases": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "catalogueId",
                  "nullable": true,
                  "references": "catalogues",
                },
                { "column_type": { "type": "Text" }, "name": "catalogNumber", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "catalogSequence",
                  "nullable": true,
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "format", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "releaseDate", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "status", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "searchKey", "nullable": false },
              ],
            },
            "teamAssignments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "teamId",
                  "nullable": false,
                  "references": "teams",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "membershipId",
                  "nullable": false,
                  "references": "memberships",
                },
                { "column_type": { "type": "Text" }, "name": "role", "nullable": false },
              ],
            },
            "teams": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "organizationId",
                  "nullable": false,
                  "references": "organizations",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "issuer",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "better_auth_account",
          },
        ],
        "fromHash": "b6640bee549149d8eaf9f9f8e7cddf486d6b3bd50cee2ddbb06a855e1980aa04",
        "toHash": "d986c626f864b1dd9e3e6a642819200d76f0e67d63da84553f95ceaa2e2ef207",
      },
    ],
  },
  "relations": {
    "artists": {
      "organization": { "one": "organizations", "column": "organizationId" },
      "releasesViaArtist": { "many": "releases", "relation": "artist" },
    },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "catalogues": {
      "organization": { "one": "organizations", "column": "organizationId" },
      "releasesViaCatalogue": { "many": "releases", "relation": "catalogue" },
    },
    "memberships": {
      "organization": { "one": "organizations", "column": "organizationId" },
      "person": { "one": "people", "column": "personId" },
      "releaseAssignmentsViaMembership": { "many": "releaseAssignments", "relation": "membership" },
      "teamAssignmentsViaMembership": { "many": "teamAssignments", "relation": "membership" },
    },
    "organizations": {
      "artistsViaOrganization": { "many": "artists", "relation": "organization" },
      "cataloguesViaOrganization": { "many": "catalogues", "relation": "organization" },
      "membershipsViaOrganization": { "many": "memberships", "relation": "organization" },
      "releaseAssignmentsViaOrganization": {
        "many": "releaseAssignments",
        "relation": "organization",
      },
      "releaseTeamsViaOrganization": { "many": "releaseTeams", "relation": "organization" },
      "releasesViaOrganization": { "many": "releases", "relation": "organization" },
      "teamAssignmentsViaOrganization": { "many": "teamAssignments", "relation": "organization" },
      "teamsViaOrganization": { "many": "teams", "relation": "organization" },
    },
    "people": {
      "membershipsViaPerson": { "many": "memberships", "relation": "person" },
      "personEmailsViaPerson": { "many": "personEmails", "relation": "person" },
    },
    "personEmails": { "person": { "one": "people", "column": "personId" } },
    "releaseAssignments": {
      "membership": { "one": "memberships", "column": "membershipId" },
      "organization": { "one": "organizations", "column": "organizationId" },
      "release": { "one": "releases", "column": "releaseId" },
    },
    "releaseTeams": {
      "organization": { "one": "organizations", "column": "organizationId" },
      "release": { "one": "releases", "column": "releaseId" },
      "team": { "one": "teams", "column": "teamId" },
    },
    "releases": {
      "artist": { "one": "artists", "column": "artistId" },
      "catalogue": { "one": "catalogues", "column": "catalogueId" },
      "organization": { "one": "organizations", "column": "organizationId" },
      "releaseAssignmentsViaRelease": { "many": "releaseAssignments", "relation": "release" },
      "releaseTeamsViaRelease": { "many": "releaseTeams", "relation": "release" },
    },
    "teamAssignments": {
      "membership": { "one": "memberships", "column": "membershipId" },
      "organization": { "one": "organizations", "column": "organizationId" },
      "team": { "one": "teams", "column": "teamId" },
    },
    "teams": {
      "organization": { "one": "organizations", "column": "organizationId" },
      "releaseTeamsViaTeam": { "many": "releaseTeams", "relation": "team" },
      "teamAssignmentsViaTeam": { "many": "teamAssignments", "relation": "team" },
    },
  },
});
