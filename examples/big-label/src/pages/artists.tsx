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
import { useAll, useDb, useOne } from "jazz-tools/react";
import { app } from "../../schema";
import { ArtistDialog, ReleaseDialog } from "../components/forms";
import {
  ListPagination,
  ListToolbar,
  StatusBadge,
  pageOf,
  sentence,
  useListControls,
} from "../components/list";
import { ConfirmButton } from "../components/confirm-button";
import { PageHeader, PageSection } from "../components/page";
import { ReleaseTable } from "./releases";
import { artistStatuses } from "../fixtures";
import { useCan, useOrganization } from "../lib/organization";
import { href, navigate } from "../lib/route";
import { useWrite } from "../lib/use-write";

export function ArtistsPage() {
  const organization = useOrganization();
  const canEdit = useCan("editCatalogue");
  const [isAdding, setIsAdding] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const list = useListControls<"name" | "genre" | "status">("name");
  const { data, isLoading } = useAll(
    app.artists
      .where({
        organizationId: organization.id,
        ...list.searchWhere,
        ...(status ? { status } : {}),
      })
      .orderBy(list.sortKey, list.direction)
      .orderBy("id", "asc")
      .limit(list.limit)
      .offset(list.offset),
  );
  const { rows, hasMore } = pageOf(data, list.pageSize);
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
  const db = useDb();
  const write = useWrite();
  const [dialog, setDialog] = useState<"edit" | "release" | null>(null);
  const { data: artist, isLoading } = useOne(
    app.artists.where({ id, organizationId: organization.id }),
  );
  // The benchmark's artist_load: one artist's releases, newest first.
  const { data: releases = [] } = useAll(
    app.releases
      .where({ organizationId: organization.id, artistId: id })
      .orderBy("releaseDate", "desc")
      .orderBy("id", "asc")
      .include({ catalogue: true })
      .limit(100),
  );

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
    write("Couldn't delete the artist", () =>
      db.delete(app.artists, artist.id).wait({ tier: "global" }),
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
