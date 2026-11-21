"use client";

import { Button, EmptyState, Grid, HStack, VStack } from "@astryxdesign/core";
import { useQuery } from "@rocicorp/zero/react";
import { COUNT_LIMIT, queries } from "../../queries";
import { formatCount } from "../components/list";
import { PageHeader, PageSection, Stat } from "../components/page";
import { roleDescriptions } from "../roles";
import { useCan, useOrganization } from "../lib/organization";
import { href } from "../lib/route";
import { ReleaseTable } from "./releases";

/** Upper bound for the overview's counts; there is no aggregate count query. */
const countLimit = COUNT_LIMIT;

/** Rows once the server has answered (undefined until then), so counts don't flicker. */
function useCount<T>([rows, result]: readonly [readonly T[], { type: string }]) {
  return result.type === "unknown" ? undefined : rows;
}

export function OverviewPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const where = { organizationId: organization.id };
  const artists = useCount(useQuery(queries.artists.ofLabel({ ...where, limit: countLimit })));
  const releases = useCount(useQuery(queries.releases.ofLabel(where)));
  const members = useCount(useQuery(queries.memberships.ofLabel({ ...where, limit: countLimit })));
  const teams = useCount(useQuery(queries.teams.ofLabel(where)));
  // The benchmark's label_load: the label's releases, newest first.
  const [latest] = useQuery(queries.releases.latest(where));
  const count = (rows: readonly unknown[] | undefined) =>
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
