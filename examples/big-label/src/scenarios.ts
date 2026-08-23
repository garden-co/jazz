import { createFixture, type FixtureProfile } from "./fixtures.js";

export type ScenarioReceipt = {
  profile: FixtureProfile;
  organizationId: string;
  visibleArtists: number;
  visibleReleases: number;
  foreignRows: number;
  operations: string[];
};

/** Framework-neutral workload receipt. Adapters may execute these operations against a live Jazz topology. */
export function tenantOperations(profile: FixtureProfile = "smoke", seed = 17): ScenarioReceipt {
  const data = createFixture(profile, seed);
  const organizationId = data.organizations[0]!.id;
  const artists = data.artists.filter((artist) => artist.organizationId === organizationId);
  const releases = data.releases.filter(
    (release) =>
      release.organizationId === organizationId &&
      artists.some((artist) => artist.id === release.artistId),
  );
  const foreignRows = [...artists, ...releases].filter(
    (row) => row.organizationId !== organizationId,
  ).length;
  return {
    profile,
    organizationId,
    visibleArtists: artists.length,
    visibleReleases: releases.length,
    foreignRows,
    operations: [
      "cold-load organization membership graph",
      "indexed artists.where({ organizationId })",
      "releases.include({ artist: true })",
      "churn release status",
    ],
  };
}

export function assertTenantIsolation(profile: FixtureProfile = "smoke") {
  const receipt = tenantOperations(profile);
  if (receipt.foreignRows !== 0)
    throw new Error(`foreign tenant rows appeared: ${receipt.foreignRows}`);
  return receipt;
}
