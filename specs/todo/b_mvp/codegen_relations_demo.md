# Codegen Relations Demo — TODO (MVP)

The example schema (`examples/todo-client-localfirst-ts/schema/current.ts`) doesn't use `col.ref()`. The generated `app.ts` therefore lacks relation types. Include types, and reverse relations. Update the example to show:

- Schema with `col.ref()` (self-referential + cross-table)
- Generated code with Include/Relations types
- Application code using `.include({ parent: true })`

> `examples/todo-client-localfirst-ts/schema/current.ts` (currently no relations)
> `packages/jazz-ts/src/codegen/codegen.test.ts:272-420` (relation analysis is tested, just not demoed)
