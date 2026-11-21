"use client";

import { useState } from "react";
import {
  Button,
  EmptyState,
  Link,
  MetadataList,
  MetadataListItem,
  Table,
  Text,
  VStack,
} from "@astryxdesign/core";
import { useQuery, useZero } from "@rocicorp/zero/react";
import { queries } from "../../queries";
import { ArtistDialog, ReleaseDialog } from "../components/forms";
import {
  ListPagination,
  ListToolbar,
  StatusBadge,
  sentence,
  useListControls,
} from "../components/list";
import { ConfirmButton } from "../components/confirm-button";
import { PageHeader, PageSection } from "../components/page";
import { ReleaseTable } from "./releases";
import { artistStatuses } from "../fixtures";
import { useCan, useOrganization } from "../lib/organization";
import { href, navigate } from "../lib/route";
import { deleteArtist } from "../lib/mutations";
import { useWrite } from "../lib/use-write";

export function ArtistsPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const [isAdding, setIsAdding] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const list = useListControls<"name" | "genre" | "status">("name");
  const [data, result] = useQuery(
    queries.artists.page({
      organizationId: organization.id,
      search: list.search,
      status,
      sort: list.sortKey,
      direction: list.direction,
      start: list.start,
      limit: list.limit,
    }),
  );
  const isLoading = data.length === 0 && result.type === "unknown";
  const { rows, hasMore } = list.pageOf(data);
  const isFiltered = Boolean(list.search || status);

  return (
    <VStack gap={5}>
      <PageHeader
        title="Artists"
        actions={
          <Button
            label="Add artist"
            isDisabled={!canEdit}
            tooltip={canEdit ? undefined : "Editors and admins can add artists"}
            onClick={() => setIsAdding(true)}
          />
        }
      />
      <ListToolbar
        searchLabel="Search artists"
        search={list.search}
        onSearch={list.setSearch}
        statuses={artistStatuses}
        status={status}
        onStatus={(value) => {
          setStatus(value);
          list.resetPage();
        }}
      />
      {!isLoading && rows.length === 0 ? (
        <EmptyState
          title={isFiltered ? "No matching artists" : "No artists yet"}
          description={
            isFiltered
              ? "Try another search or status."
              : "Add your first artist, or load demo data from Settings."
          }
        />
      ) : (
        <Table
          density="compact"
          data={rows}
          idKey="id"
          plugins={list.plugins}
          columns={[
            {
              key: "name",
              header: "Name",
              sortable: true,
              renderCell: (artist) => <Link href={href.artist(artist.id)}>{artist.name}</Link>,
            },
            { key: "genre", header: "Genre", sortable: true },
            {
              key: "status",
              header: "Status",
              sortable: true,
              renderCell: (artist) => <StatusBadge status={artist.status} />,
            },
          ]}
        />
      )}
      <ListPagination page={list.page} onChange={list.setPage} hasMore={hasMore} />
      {isAdding && (
        <ArtistDialog
          isOpen
          onOpenChange={setIsAdding}
          onCreated={(id) => navigate(href.artist(id))}
        />
      )}
    </VStack>
  );
}

export function ArtistPage({ id }: { id: string }) {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const canDelete = useCan("deleteCatalogue");
  const write = useWrite();
  const [dialog, setDialog] = useState<"edit" | "release" | null>(null);
  const db = useZero();
  const [artist, artistResult] = useQuery(queries.artists.one({ organizationId: organization.id, id }));
  const isLoading = !artist && artistResult.type !== "complete";
  // The benchmark's artist_load: one artist's releases, newest first.
  const [releases] = useQuery(queries.releases.ofArtist({ organizationId: organization.id, artistId: id }));

  if (!artist)
    return isLoading ? null : (
      <EmptyState
        title="Artist not found"
        description="It may have been deleted, or it belongs to another label."
        actions={<Button label="Back to artists" href={href.artists} variant="secondary" />}
      />
    );

  const remove = () => {
    if (releases.length > 0) return;
    write("Couldn't delete the artist", async () =>
      deleteArtist(db, organization.id, artist.id),
    );
    navigate(href.artists);
  };

  return (
    <VStack gap={6}>
      <PageHeader
        title={artist.name}
        parent={{ label: "Artists", href: href.artists }}
        actions={
          <>
            {canEdit && (
              <Button label="Edit" variant="secondary" onClick={() => setDialog("edit")} />
            )}
            {canDelete &&
              (releases.length > 0 ? (
                <Button
                  label="Delete"
                  variant="destructive"
                  isDisabled
                  tooltip="Delete this artist's releases first"
                />
              ) : (
                <ConfirmButton
                  label="Delete"
                  title={`Delete ${artist.name}?`}
                  description="The artist is removed for everyone in this label."
                  onConfirm={remove}
                />
              ))}
          </>
        }
      />
      <MetadataList columns="multi">
        <MetadataListItem label="Genre">{artist.genre}</MetadataListItem>
        <MetadataListItem label="Status">
          <StatusBadge status={artist.status} />
        </MetadataListItem>
        <MetadataListItem label="Releases">{releases.length}</MetadataListItem>
      </MetadataList>
      <PageSection
        title="Releases"
        actions={
          canEdit && (
            <Button label="Add release" variant="secondary" onClick={() => setDialog("release")} />
          )
        }
      >
        {releases.length === 0 ? (
          <Text color="secondary">{sentence(artist.name)} has no releases yet.</Text>
        ) : (
          <ReleaseTable releases={releases} hideArtist />
        )}
      </PageSection>
      {dialog === "edit" && (
        <ArtistDialog isOpen onOpenChange={() => setDialog(null)} artist={artist} />
      )}
      {dialog === "release" && (
        <ReleaseDialog
          isOpen
          onOpenChange={() => setDialog(null)}
          artistId={artist.id}
          onCreated={(releaseId) => navigate(href.release(releaseId))}
        />
      )}
    </VStack>
  );
}
