"use client";

import { Button, EmptyState, Grid, HStack, VStack } from "@astryxdesign/core";
import { useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import { formatCount } from "../components/list";
import { PageHeader, PageSection, Stat } from "../components/page";
import { roleDescriptions } from "../roles";
import { useCan, useOrganization } from "../lib/organization";
import { href } from "../lib/route";
import { ReleaseTable } from "./releases";

export function OverviewPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  // Bounded counts (limit 5000), computed by the server.
  const counts = useQuery(api.overview.counts, { organizationId: organization.id });
  // The benchmark's label_load: the label's releases, newest first.
  const latest = useQuery(api.releases.latest, { organizationId: organization.id, limit: 8 }) ?? [];
  const count = (value: { count: number; isCapped: boolean } | undefined) =>
    value ? formatCount(value.count, value.isCapped) : "–";

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
          note={`${formatCount(counts?.scheduled ?? 0, counts?.releases.isCapped ?? false)} scheduled`}
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
