# `wasmColumnToAst` silently drops `default` and `mergeStrategy`

## What

`wasmColumnToAst` in `packages/jazz-tools/src/schema-loader.ts` projects a `ColumnDescriptor` to an AST `Column` but only carries `name`, `sqlType`, `nullable`, and `references`. `default` and `mergeStrategy` are dropped on the floor.

PR #665 sidesteps this for the wasm hash by preserving the original `wasmSchema` from `defineApp` instead of regenerating one from the lossy AST. But any consumer that reads `LoadedSchemaProject.schema` (the AST) for those fields will silently see the wrong value. Today nothing in `cli.ts` / `dev-server.ts` reads them — only table names — so this is latent, not active.

## Priority

low

## Notes

- File: `packages/jazz-tools/src/schema-loader.ts:89-96`
- Either fix the round-trip in `wasmColumnToAst` (carry `default` and `mergeStrategy` through) or remove the AST round-trip entirely and have `wasmSchemaToAst` return a tagged structure that callers can't accidentally trust for these fields.
- Surfaced during review of PR #665.
