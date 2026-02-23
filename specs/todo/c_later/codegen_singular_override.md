# Codegen $singular Override — TODO (Later)

Allow users to override the auto-singularised interface name for a table via a `$singular` annotation in the schema DSL.

## Motivation

Even with a proper inflection library, English has edge cases. A user escape hatch means no one is ever stuck with a wrong generated name.

This also matters for non-English speakers. Table names may follow naming conventions from the user's own language, where English singularisation rules produce nonsense. A manual override lets users write whatever works best in their own language rather than enforcing English.

## Proposed API

```typescript
table("canvases", { $singular: "Canvas", name: col.string() });
```

If `$singular` is present, `tableNameToInterface` uses it directly (PascalCased) instead of running singularisation.

## Open questions

- Should `$singular` be passed through the WASM schema boundary, or handled purely in the TS codegen layer?
- Naming: `$singular` vs `$typeName` vs `$as`?
