export interface SchemaHashInfo {
  hash: string;
  publishedAt: number | null;
}

export function normalizeSchemaHashInfos(
  hashes: string[],
  schemas: readonly SchemaHashInfo[] = [],
): SchemaHashInfo[] {
  const schemaByHash = new Map(schemas.map((schema) => [schema.hash, schema]));
  const hashOrder = new Map(hashes.map((hash, index) => [hash, index]));
  return hashes
    .map((hash) => schemaByHash.get(hash) ?? { hash, publishedAt: null })
    .sort((left, right) => {
      if (left.publishedAt === null && right.publishedAt === null) {
        return (hashOrder.get(left.hash) ?? 0) - (hashOrder.get(right.hash) ?? 0);
      }
      if (left.publishedAt === null) return 1;
      if (right.publishedAt === null) return -1;
      return right.publishedAt - left.publishedAt;
    });
}

export function shortSchemaHash(hash: string): string {
  return hash.slice(0, 12);
}

export function formatSchemaPublishedAt(publishedAt: number | null): string | null {
  if (publishedAt === null || !Number.isFinite(publishedAt)) {
    return null;
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(publishedAt));
}

export function formatSchemaHashOptionLabel(schema: SchemaHashInfo): string {
  const publishedAt = formatSchemaPublishedAt(schema.publishedAt);
  if (!publishedAt) {
    return shortSchemaHash(schema.hash);
  }

  return `${shortSchemaHash(schema.hash)} - uploaded ${publishedAt}`;
}
