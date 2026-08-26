# Jamazon Warehouse

Jamazon Warehouse is a self-contained operations console for the fictional music-instrument
storefront. Its schema and scenarios are deliberately TPC-C-shaped: warehouses, districts,
stock, customers, orders, order lines, payments, and delivery work.

The app is a reference for multi-row exclusive checkout, indexed operational reads, local-first
retry, and idempotent external-effect handoff. It is not a TPC-C compliance claim. `benchmarks/`
duplicates the schema/query shapes in a deterministic Divan fixture.

Stock-level reads currently fetch complete warehouse candidates and apply `on_hand < reorder_level`
in the fixture because Jazz does not yet lower field-to-field comparisons; [#1864](https://github.com/garden-co/jazz/issues/1864)
tracks the indexed query path.

The current browser scenario supplies deterministic test credentials directly so it can isolate
Jazz topology behavior. A user-facing Next.js + Better Auth shell is intentionally not claimed
yet; it will be added once it can exercise the same checkout flow rather than a parallel toy
path.
