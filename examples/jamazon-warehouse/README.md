# Jamazon Warehouse

Jamazon Warehouse is a self-contained operations console for the fictional music-instrument
storefront. Its schema and scenarios are deliberately TPC-C-shaped: warehouses, districts,
stock, customers, orders, order lines, payments, and delivery work.

The app is a reference for multi-row exclusive checkout, indexed operational reads, concurrent
workers, local-first retry, and idempotent external-effect handoff. It is not a TPC-C compliance
claim. `benchmarks/` duplicates the schema/query shapes in a deterministic Divan fixture.

The canonical UI is a Next.js + Better Auth app. Set the normal `create-jazz` Next/Better Auth
environment variables before running it.
