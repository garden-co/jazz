// Well-known metadata keys and values used across object and commit metadata.
import { SortedMap, cmpStr } from "../../support/collections.js";

export const MetadataKey = {
  Table: "table",
  OriginSchemaHash: "origin_schema_hash",
  Type: "type",
  Delete: "delete",
  CreatedBy: "created_by",
  CreatedAt: "created_at",
  AppId: "app_id",
  SchemaHash: "schema_hash",
  PublishedAt: "published_at",
  SourceHash: "source_hash",
  TargetHash: "target_hash",
  NoSync: "nosync",
} as const;

export const ObjectType = {
  CatalogueSchema: "catalogue_schema",
  CatalogueLens: "catalogue_lens",
  CataloguePermissionsBundle: "catalogue_permissions_bundle",
  CataloguePermissionsHead: "catalogue_permissions_head",
  Index: "index",
} as const;

export const isCatalogueTypeStr = (kind: string) =>
  kind === "catalogue_schema" ||
  kind === "catalogue_lens" ||
  kind === "catalogue_permissions_bundle" ||
  kind === "catalogue_permissions_head";

export type DeleteKind = "soft" | "hard";

export const SYSTEM_PRINCIPAL_ID = "jazz:system";

export type RowProvenance = {
  created_by: string;
  created_at: number;
  updated_by: string;
  updated_at: number;
};

export const provenanceForInsert = (author: string, ts: number): RowProvenance => ({
  created_by: author,
  created_at: ts,
  updated_by: author,
  updated_at: ts,
});
export const provenanceForUpdate = (
  existing: RowProvenance,
  author: string,
  ts: number,
): RowProvenance => ({
  created_by: existing.created_by,
  created_at: existing.created_at,
  updated_by: author,
  updated_at: ts,
});

export type Metadata = SortedMap<string, string>;

export function rowProvenanceMetadata(p: RowProvenance, deleteKind?: DeleteKind): Metadata {
  const m = new SortedMap<string, string>(cmpStr, [
    [MetadataKey.CreatedBy, p.created_by],
    [MetadataKey.CreatedAt, String(p.created_at)],
  ]);
  if (deleteKind) m.set(MetadataKey.Delete, deleteKind);
  return m;
}

export function rowProvenanceFromMetadata(
  metadata: Metadata | undefined,
  updated_by: string,
  updated_at: number,
): RowProvenance | undefined {
  if (!metadata) return undefined;
  const created_by = metadata.get(MetadataKey.CreatedBy);
  const raw = metadata.get(MetadataKey.CreatedAt);
  if (created_by === undefined || raw === undefined || !/^\+?\d+$/.test(raw)) return undefined;
  return { created_by, created_at: Number(raw), updated_by, updated_at };
}

export const softDeleteMetadata = (): Metadata =>
  new SortedMap<string, string>(cmpStr, [[MetadataKey.Delete, "soft"]]);
export const hardDeleteMetadata = (): Metadata =>
  new SortedMap<string, string>(cmpStr, [[MetadataKey.Delete, "hard"]]);
