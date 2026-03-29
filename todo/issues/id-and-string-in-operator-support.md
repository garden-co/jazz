# ID and string IN operator support

## What

The public query surface documents `id: { in: [...] }` as supported, but the core Jazz query path does not have clear end-to-end coverage for it. We also need string-column `in` support in the core path so batch string lookups can stay queryable instead of degrading into client-side filtering in optimization-sensitive integrations.

## Where

- `docs/content/docs/reading-data.mdx`
- `docs/content/partials/quickstarts/where-operators-table.mdx`
- `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter.ts`
- `packages/jazz-tools/src/better-auth-adapter/utils.ts`

## Steps to reproduce

1. Read the docs/operator table: `id` is documented as supporting `eq`, `ne`, and `in`.
2. Look for a core Jazz query test that exercises `where({ id: { in: [...] } })`.
3. Compare that with current query support for batch string-key lookups.

## Expected

`id.in` is implemented and covered in the main Jazz query/runtime path to match the docs. String-column `in` is also available where the engine can use it for efficient batched lookups.

## Actual

`id.in` is documented, but support is not clearly proven in the core public query path. String-column `in` is also missing in places where it would unlock engine-side filtering and avoid client-side fallbacks.

## Priority

medium

## Notes

Keep this as one directionally-related issue: first align `id.in` with the documented contract, then extend the same path to text/string `in` for performance-oriented integrations.
