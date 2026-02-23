# Codegen Singularisation — TODO (MVP)

Table names are plural by convention (`todos`, `user_profiles`, `canvases`). The codegen singularises the last word to produce TypeScript interface names (`Todo`, `UserProfile`, `Canvas`). The current implementation is a handful of suffix rules that fail on common English words.

## Problem

`singularize()` in `type-generator.ts:53-78` uses four branches:

1. `ies` → strip, add `y`
2. `es` after `ss/x/ch/sh` → strip `es`
3. Other `es` → strip just `s`
4. Trailing `s` (not `ss`) → strip `s`

Branch 3 produces wrong output for any word where `es` is the full plural suffix: `canvases` → `canvase`, `statuses` → `statuse`, `buses` → `buse`, `processes` → `processe`, `heroes` → `heroe`, `potatoes` → `potatoe`, `vertices` → `vertice`.

It also has no handling for irregular plurals (`people`, `geese`, `mice`, `indices`, `data`).

## Design

Replace the hand-rolled function with [`pluralize-esm`](https://www.npmjs.com/package/pluralize-esm) (MIT, ESM fork of `pluralize` maintained by Sanity.io, zero dependencies, ships with types). It handles all the suffix rules we'd need to write ourselves plus hundreds of irregular forms, and exposes `addSingularRule()` / `addIrregularRule()` for extending.

All usage is codegen-only (`type-generator.ts` and `query-builder-generator.ts`), so `pluralize-esm` is a devDependency.

## Changes

### 1. Add dependency

Add `pluralize-esm` as a devDependency to `packages/jazz-tools`.

### 2. Replace `singularize()`

```typescript
import pluralize from "pluralize-esm";

function singularize(word: string): string {
  return pluralize.singular(word);
}
```

`tableNameToInterface` stays the same (split on `_`, singularise last part, PascalCase join).

### 3. Update tests

Rename the existing test from "removes trailing s for plurals" to something more descriptive. Add cases that previously broke:

- `canvases` → `Canvas`
- `statuses` → `Status`
- `buses` → `Bus`
- `processes` → `Process`
- `heroes` → `Hero`
- `vertices` → `Vertex`
- `people` → `Person`
- `matrices` → `Matrix`
- `addresses` → `Address`

Keep existing passing cases (`todos` → `Todo`, `categories` → `Category`, `user_profiles` → `UserProfile`).

## Files touched

- `packages/jazz-tools/package.json` — new devDependency
- `packages/jazz-tools/src/codegen/type-generator.ts` — replace `singularize()`
- `packages/jazz-tools/src/codegen/codegen.test.ts` — expanded test cases
