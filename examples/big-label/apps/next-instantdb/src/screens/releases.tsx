"use client";

import { useState } from "react";
import {
  Button,
  EmptyState,
  HStack,
  Icon,
  IconButton,
  Link,
  List,
  ListItem,
  MetadataList,
  MetadataListItem,
  Selector,
  Table,
  Text,
  VStack,
} from "@astryxdesign/core";
import { id as newId } from "@instantdb/react";
import { db } from "../lib/db";
import { ReleaseDialog } from "../components/forms";
import {
  ListPagination,
  ListToolbar,
  StatusBadge,
  formatDate,
  pageOf,
  useListControls,
} from "../components/list";
import { ConfirmButton } from "../components/confirm-button";
import { PageHeader, PageSection } from "../components/page";
import { releaseStatuses } from "../fixtures";
import { useCan, useOrganization } from "../lib/organization";
import { href, navigate } from "../lib/route";
import { deleteRelease } from "../lib/mutations";
import { useWrite } from "../lib/use-write";

type ReleaseRow = {
  id: string;
  title: string;
  catalogNumber: string;
  releaseDate: Date;
  status: string;
  format: string;
  artist?: { id: string; name: string } | null;
};

/** Releases in a compact table; used by the label, artist and catalogue pages. */
export function ReleaseTable({
  releases,
  hideArtist = false,
  plugins,
}: {
  releases: ReleaseRow[];
  hideArtist?: boolean;
  plugins?: ReturnType<typeof useListControls>["plugins"];
}) {
  return (
    <Table
      density="compact"
      data={releases}
      idKey="id"
      plugins={plugins}
      columns={[
        {
          key: "title",
          header: "Title",
          sortable: true,
          renderCell: (release) => <Link href={href.release(release.id)}>{release.title}</Link>,
        },
        ...(hideArtist
          ? []
          : [
              {
                key: "artist",
                header: "Artist",
                renderCell: (release: ReleaseRow) =>
                  release.artist ? (
                    <Link href={href.artist(release.artist.id)}>{release.artist.name}</Link>
                  ) : null,
              },
            ]),
        { key: "catalogNumber", header: "Cat. no.", sortable: true },
        {
          key: "releaseDate",
          header: "Release date",
          sortable: true,
          renderCell: (release) => formatDate(release.releaseDate),
        },
        {
          key: "status",
          header: "Status",
          sortable: true,
          renderCell: (release) => <StatusBadge status={release.status} />,
        },
      ]}
    />
  );
}

export function ReleasesPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const [isAdding, setIsAdding] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const list = useListControls<"title" | "catalogNumber" | "releaseDate" | "status">(
    "releaseDate",
    "descending",
  );
  const { data, isLoading } = db.useQuery({
    releases: {
      $: {
        where: {
          organization: organization.id,
          ...list.searchWhere,
          ...(status ? { status } : {}),
        },
        order: { [list.sortKey]: list.direction },
        limit: list.limit,
        offset: list.offset,
      },
      artist: {},
    },
  });
  const { rows, hasMore } = pageOf(data?.releases, list.pageSize);
  const isFiltered = Boolean(list.search || status);

  return (
    <VStack gap={5}>
      <PageHeader
        title="Releases"
        actions={
          <Button
            label="Add release"
            isDisabled={!canEdit}
            tooltip={canEdit ? undefined : "Editors and admins can add releases"}
            onClick={() => setIsAdding(true)}
          />
        }
      />
      <ListToolbar
        searchLabel="Search title or catalogue number"
        search={list.search}
        onSearch={list.setSearch}
        statuses={releaseStatuses}
        status={status}
        onStatus={(value) => {
          setStatus(value);
          list.resetPage();
        }}
      />
      {!isLoading && rows.length === 0 ? (
        <EmptyState
          title={isFiltered ? "No matching releases" : "No releases yet"}
          description={
            isFiltered ? "Try another search or status." : "Add an artist first, then a release."
          }
        />
      ) : (
        <ReleaseTable releases={rows} plugins={list.plugins} />
      )}
      <ListPagination page={list.page} onChange={list.setPage} hasMore={hasMore} />
      {isAdding && (
        <ReleaseDialog
          isOpen
          onOpenChange={setIsAdding}
          onCreated={(id) => navigate(href.release(id))}
        />
      )}
    </VStack>
  );
}

export function ReleasePage({ id }: { id: string }) {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const canAssign = useCan("assignReleases");
  const canDelete = useCan("deleteCatalogue");
  const write = useWrite();
  const [isEditing, setIsEditing] = useState(false);
  const [teamId, setTeamId] = useState<string | null>(null);
  const { data, isLoading } = db.useQuery({
    releases: {
      $: { where: { id, organization: organization.id } },
      artist: {},
      catalogue: {},
    },
    releaseTeams: { $: { where: { organization: organization.id, release: id } }, team: {} },
    teams: { $: { where: { organization: organization.id }, order: { name: "asc" } } },
  });
  const row = data?.releases[0];
  const release = row && {
    ...row,
    artistId: row.artist?.id ?? "",
    catalogueId: row.catalogue?.id ?? null,
  };
  const assignments = (data?.releaseTeams ?? []).map((assignment) => ({
    ...assignment,
    teamId: assignment.team?.id ?? "",
  }));
  const teams = data?.teams ?? [];

  if (!release)
    return isLoading ? null : (
      <EmptyState
        title="Release not found"
        description="It may have been deleted, or it belongs to another label."
        actions={<Button label="Back to releases" href={href.releases} variant="secondary" />}
      />
    );

  const assignedTeamIds = new Set(assignments.map((assignment) => assignment.teamId));
  const unassignedTeams = teams.filter((team) => !assignedTeamIds.has(team.id));

  const assign = () => {
    if (!teamId) return;
    write("Couldn't assign the team", () =>
      db.transact(
        db.tx.releaseTeams[newId()]
          .create({})
          .link({ organization: organization.id, release: id, team: teamId }),
      ),
    );
    setTeamId(null);
  };
  const remove = () => {
    write("Couldn't delete the release", () => deleteRelease(id));
    navigate(href.releases);
  };

  return (
    <VStack gap={6}>
      <PageHeader
        title={release.title}
        parent={{ label: "Releases", href: href.releases }}
        actions={
          <>
            {canEdit && (
              <Button label="Edit" variant="secondary" onClick={() => setIsEditing(true)} />
            )}
            {canDelete && (
              <ConfirmButton
                label="Delete"
                title={`Delete ${release.title}?`}
                description="The release and its team assignments are removed for everyone."
                onConfirm={remove}
              />
            )}
          </>
        }
      />
      <MetadataList columns="multi">
        <MetadataListItem label="Artist">
          {release.artist ? (
            <Link href={href.artist(release.artist.id)}>{release.artist.name}</Link>
          ) : (
            "Unknown"
          )}
        </MetadataListItem>
        <MetadataListItem label="Catalogue number">{release.catalogNumber}</MetadataListItem>
        <MetadataListItem label="Catalogue">
          {release.catalogue ? (
            <Link href={href.catalogue(release.catalogue.id)}>{release.catalogue.name}</Link>
          ) : (
            "None"
          )}
        </MetadataListItem>
        <MetadataListItem label="Format">{release.format}</MetadataListItem>
        <MetadataListItem label="Release date">{formatDate(release.releaseDate)}</MetadataListItem>
        <MetadataListItem label="Status">
          <StatusBadge status={release.status} />
        </MetadataListItem>
      </MetadataList>
      <PageSection title="Teams">
        {assignments.length === 0 ? (
          <Text color="secondary">No team is working on this release yet.</Text>
        ) : (
          <List hasDividers>
            {assignments.map((assignment) => (
              <ListItem
                key={assignment.id}
                label={assignment.team?.name ?? "Team"}
                href={href.team(assignment.teamId)}
                endContent={
                  canAssign && (
                    <IconButton
                      label={`Remove ${assignment.team?.name ?? "team"}`}
                      icon={<Icon icon="close" size="sm" />}
                      variant="ghost"
                      size="sm"
                      onClick={() =>
                        write("Couldn't remove the team", () =>
                          db.transact(db.tx.releaseTeams[assignment.id].delete()),
                        )
                      }
                    />
                  )
                }
              />
            ))}
          </List>
        )}
        {canAssign && unassignedTeams.length > 0 && (
          <HStack gap={2} vAlign="end" wrap="wrap">
            <Selector
              label="Assign a team"
              placeholder="Choose a team"
              options={unassignedTeams.map((team) => ({ value: team.id, label: team.name }))}
              value={teamId}
              onChange={setTeamId}
              hasClear
              width={240}
            />
            <Button label="Assign" variant="secondary" isDisabled={!teamId} onClick={assign} />
          </HStack>
        )}
      </PageSection>
      {isEditing && <ReleaseDialog isOpen onOpenChange={setIsEditing} release={release} />}
    </VStack>
  );
}
