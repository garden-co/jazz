# Built-in File Storage: Composability with E2EE — TODO

The built-in file storage helper (`createFileStorage` / `createConventionalFileStorage`) doesn't compose with E2EE because it hardcodes the insert payloads — `{ data: chunk }` for parts, `{ mimeType, partIds, partSizes, ... }` for files — giving the caller no way to inject per-row scope columns (e.g. `orgId` for `data: s.bytes().encrypted("orgId")`). Apps that need encrypted files fall off the paved path entirely and reimplement chunking, reassembly, streaming, and integrity checks by hand.

## What Changes

`FileWriteOptions` gains two new optional fields:

```typescript
export interface FileWriteOptions {
  // ... existing fields (name, mimeType, chunkSizeBytes, tier) ...

  /** Extra columns merged into every file-part insert row.
   *  Spread FIRST, so built-in "data" always wins over any key collision. */
  partData?: Record<string, unknown>;
  /** Extra columns merged into the file-index insert row.
   *  Spread FIRST, so built-in "partIds" / "partSizes" / "mimeType" always win. */
  fileData?: Record<string, unknown>;
}
```

In `fromStream()`, the part insert becomes:

```typescript
{ ...writeOptions.partData, [columns.data]: chunk }
```

And the file-index insert becomes:

```typescript
{
  ...writeOptions.fileData,
  [columns.mimeType]: writeOptions.mimeType ?? DEFAULT_MIME_TYPE,
  [columns.partIds]: filepartIds,
  [columns.partSizes]: partSizes,
  ...(writeOptions.name !== undefined ? { [columns.name]: writeOptions.name } : {}),
}
```

No other methods (`toStream`, `toBlob`, `fromBlob`) change except `fromBlob` already delegates to `fromStream`.

## Usage

Encrypted-file apps replace hand-rolled chunking with the built-in helper:

```typescript
const storage = createConventionalFileStorage(db, app);
const file = await storage.fromBlob(blob, {
  name: blob.name,
  mimeType: "application/pdf",
  fileData: { orgId, title, uploadedBy, createdAt },
  partData: { orgId },
});
// file has type ConventionalFileRow<typeof app> — no cast needed
```

## Migration

- **The `encrypted-documents-react` example** can delete `src/pdf-storage.ts` (~120 lines) and use `createConventionalFileStorage` directly. Gains streaming, atomicity, and integrity checks for free.
- **No breaking changes** — the new fields are optional and default to `undefined`, so existing callers are unaffected.

## Non-goals (tracked separately)

- Cascade delete — see `file_storage_cascade_integration.md`.
- Type-level derivation of `partData` / `fileData` from the table schema — kept as `Record<string, unknown>` for simplicity. A future pass could compute required extra columns from `TableProxy<_, Init>` and enforce them at the type level, but that requires deeper generics gymnastics.
