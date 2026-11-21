"use client";

import { Button, EmptyState, Grid, HStack, VStack } from "@astryxdesign/core";
import { useAll, useOne } from "../lib/live";
import { queries } from "../queries";
import { formatCount } from "../components/list";
import { PageHeader, PageSection, Stat } from "../components/page";
import { roleDescriptions } from "../roles";
import { useCan, useOrganization } from "../lib/organization";
import { href } from "../lib/route";
import { ReleaseTable } from "./releases";

/** Upper bound for the overview's counts (the server counts bounded reads). */
const countLimit = 5000;

export function OverviewPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const where = { organizationId: organization.id };
  // Each count is of a bounded read (at most countLimit rows), as on Jazz.
  const { data: counts } = useOne(queries.labels.counts(where));
  // The benchmark's label_load: the label's releases, newest first.
  const { data: latest = [] } = useAll(queries.releases.latest(where));
  const count = (value: number | undefined) =>
    value === undefined ? "–" : formatCount(value, value >= countLimit);
  const scheduled = counts?.scheduled;

  return (
    <VStack gap={6}>
      <PageHeader
        title={organization.name}
        description={`You are ${roleDescriptions[organization.role]} of this label.`}
      />
      <Grid columns={{ minWidth: 160 }} gap={3}>
        <Stat label="Artists" value={count(counts?.artists)} />
        <Stat
          label="Releases"
          value={count(counts?.releases)}
          note={`${formatCount(scheduled ?? 0, (counts?.releases ?? 0) >= countLimit)} scheduled`}
        />
        <Stat label="Members" value={count(counts?.members)} />
        <Stat label="Teams" value={count(counts?.teams)} />
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
