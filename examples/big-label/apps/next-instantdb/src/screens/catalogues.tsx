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
import { db } from "../lib/db";
import { CatalogueDialog } from "../components/forms";
import { ListPagination, pageOf, useListControls } from "../components/list";
import { PageHeader, PageSection } from "../components/page";
import { useCan, useOrganization } from "../lib/organization";
import { href } from "../lib/route";
import { ReleaseTable } from "./releases";

export function CataloguesPage() {
  const organization = useOrganization();
  const canEdit = useCan("editSettings");
  const [isAdding, setIsAdding] = useState(false);
  const { data, isLoading } = db.useQuery({
    catalogues: { $: { where: { organization: organization.id }, order: { name: "asc" } } },
  });
  const catalogues = data?.catalogues;
  return (
    <VStack gap={5}>
      <PageHeader
        title="Catalogues"
        description="Series that group releases and number them."
        actions={
          <Button
            label="Add catalogue"
            isDisabled={!canEdit}
            tooltip={canEdit ? undefined : "Admins can add catalogues"}
            onClick={() => setIsAdding(true)}
          />
        }
      />
      {!isLoading && catalogues?.length === 0 ? (
        <EmptyState title="No catalogues yet" description="Add one to number your releases." />
      ) : (
        <Table
          density="compact"
          data={catalogues ?? []}
          idKey="id"
          columns={[
            {
              key: "name",
              header: "Name",
              renderCell: (catalogue) => (
                <Link href={href.catalogue(catalogue.id)}>{catalogue.name}</Link>
              ),
            },
            { key: "code", header: "Code" },
          ]}
        />
      )}
      {isAdding && <CatalogueDialog isOpen onOpenChange={setIsAdding} />}
    </VStack>
  );
}

export function CataloguePage({ id }: { id: string }) {
  const organization = useOrganization();
  const canEdit = useCan("editSettings");
  const [isEditing, setIsEditing] = useState(false);
  const list = useListControls<"title" | "catalogNumber" | "releaseDate" | "status">(
    "releaseDate",
    "descending",
  );
  const { data: catalogueData, isLoading } = db.useQuery({
    catalogues: { $: { where: { id, organization: organization.id } } },
  });
  const catalogue = catalogueData?.catalogues[0];
  // The benchmark's catalog_load: one catalogue's releases, ordered and paged.
  const { data } = db.useQuery({
    releases: {
      $: {
        where: { organization: organization.id, catalogue: id },
        order: { [list.sortKey]: list.direction },
        limit: list.limit,
        offset: list.offset,
      },
      artist: {},
    },
  });
  const { rows, hasMore } = pageOf(data?.releases, list.pageSize);

  if (!catalogue)
    return isLoading ? null : (
      <EmptyState
        title="Catalogue not found"
        actions={<Button label="Back to catalogues" href={href.catalogues} variant="secondary" />}
      />
    );

  return (
    <VStack gap={6}>
      <PageHeader
        title={catalogue.name}
        parent={{ label: "Catalogues", href: href.catalogues }}
        actions={
          canEdit && <Button label="Edit" variant="secondary" onClick={() => setIsEditing(true)} />
        }
      />
      <MetadataList columns="multi">
        <MetadataListItem label="Code">{catalogue.code}</MetadataListItem>
      </MetadataList>
      <PageSection title="Releases">
        {rows.length === 0 ? (
          <Text color="secondary">No releases in this catalogue yet.</Text>
        ) : (
          <ReleaseTable releases={rows} plugins={list.plugins} />
        )}
        <ListPagination page={list.page} onChange={list.setPage} hasMore={hasMore} />
      </PageSection>
      {isEditing && <CatalogueDialog isOpen onOpenChange={setIsEditing} catalogue={catalogue} />}
    </VStack>
  );
}
