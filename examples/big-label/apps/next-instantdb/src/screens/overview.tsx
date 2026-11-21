"use client";

import { Button, EmptyState, Grid, HStack, VStack } from "@astryxdesign/core";
import { db } from "../lib/db";
import { formatCount } from "../components/list";
import { PageHeader, PageSection, Stat } from "../components/page";
import { roleDescriptions } from "../roles";
import { useCan, useOrganization } from "../lib/organization";
import { href } from "../lib/route";
import { ReleaseTable } from "./releases";

/** Upper bound for the overview's counts; there is no aggregate count query. */
const countLimit = 5000;

export function OverviewPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const where = { organization: organization.id };
  const { data: counts } = db.useQuery({
    artists: { $: { where, fields: ["id"], limit: countLimit } },
    memberships: { $: { where, fields: ["id"], limit: countLimit } },
    teams: { $: { where, fields: ["id"], limit: countLimit } },
  });
  const { data: releaseData } = db.useQuery({
    releases: { $: { where, fields: ["status"], limit: countLimit } },
  });
  // The benchmark's label_load: the label's releases, newest first.
  const { data: latestData } = db.useQuery({
    releases: { $: { where, order: { releaseDate: "desc" }, limit: 8 }, artist: {} },
  });
  const artists = counts?.artists;
  const members = counts?.memberships;
  const teams = counts?.teams;
  const releases = releaseData?.releases;
  const latest = latestData?.releases ?? [];
  const count = (rows: unknown[] | undefined) =>
    rows ? formatCount(rows.length, rows.length >= countLimit) : "–";
  const scheduled = releases?.filter((release) => release.status === "scheduled").length;

  return (
    <VStack gap={6}>
      <PageHeader
        title={organization.name}
        description={`You are ${roleDescriptions[organization.role]} of this label.`}
      />
      <Grid columns={{ minWidth: 160 }} gap={3}>
        <Stat label="Artists" value={count(artists)} />
        <Stat
          label="Releases"
          value={count(releases)}
          note={`${formatCount(scheduled ?? 0, (releases?.length ?? 0) >= countLimit)} scheduled`}
        />
        <Stat label="Members" value={count(members)} />
        <Stat label="Teams" value={count(teams)} />
      </Grid>
      <PageSection
        title="Latest releases"
        actions={<Button label="All releases" variant="secondary" href={href.releases} />}
      >
        {latest.length === 0 ? (
          <EmptyState
            isCompact
            title="No releases yet"
            description="Add artists and releases, or load a demo dataset to explore a full label."
            actions={
              <HStack gap={2} wrap="wrap" justify="center">
                {canEdit && <Button label="Add artist" href={href.artists} variant="secondary" />}
                <Button label="Load demo data" href={href.settings} variant="secondary" />
              </HStack>
            }
          />
        ) : (
          <ReleaseTable releases={latest} />
        )}
      </PageSection>
    </VStack>
  );
}
