# Jamazon Warehouse

Jamazon Warehouse is the operations console for a fictional music-instrument storefront. Its
schema and workflows are deliberately TPC-C-shaped: warehouses, districts, stock, customers,
orders, order lines, payments and deliveries. It is not a TPC-C compliance claim.

What an operator does in the console:

- **Dashboard**: orders entered today, pending deliveries and low-stock items for the warehouse,
  plus the next orders to deliver in the selected district. Every number updates live.
- **New order** (TPC-C "new order"): pick a customer and up to 15 item lines. Checkout is two
  exclusive transactions (see [Two-phase checkout](#two-phase-checkout)). Each attempt carries a
  request key, so a retry (or "submit the same request again") returns the first receipt instead
  of taking stock twice.
- **Pending orders**: the district's delivery queue, oldest first, one bounded page.
- **Delivery** (TPC-C "delivery"): one exclusive batch delivers the oldest pending order in every
  district.
- **Payment** (TPC-C "payment"): credit a customer's balance, once per request key.
- **Order status** (TPC-C "order status"): a customer's recent orders, or one order by number,
  with its lines and deliveries.
- **Stock level** (TPC-C "stock level"): items below their reorder level, with a one-click
  receipt of new stock.

## Running it

```sh
pnpm install
pnpm --dir examples/jamazon-warehouse dev
```

`pnpm dev` starts a local Jazz server through `withJazz` and pushes `schema.ts` and
`permissions.ts` to it. Open <http://localhost:3000>, create an operator account and choose a
warehouse. Open a second browser (or a private window) and sign up a second operator to watch the
queue and stock change live in both.

The first console open calls `POST /api/bootstrap`, which seeds the deterministic small profile
once (`src/seed.ts`): two warehouses with three districts each, four customers per district, a
16-item catalogue, stock for every item and five orders per district. Joining a warehouse goes
through the same route, which verifies the operator's Better Auth session and Jazz account and
then adds a `warehouse_operators` row with backend authority.

Joining is deliberately open in this demo: any signed-in account can staff itself on any
warehouse, once, through `/api/bootstrap` (`ensureOperator` in `src/seed.ts`). A real deployment
would have the manager invite operators instead; the permissions already allow that, since only a
warehouse's manager (or backend authority) may insert its `warehouse_operators` rows.

To see stock contention, have both operators order 4 "Vintage tube amp" (5 on hand) at the same
moment. The authority accepts one order; the other checkout re-reads stock and fails with
"Insufficient stock", with nothing charged or taken.

Configuration fails closed (`src/lib/config.ts`). A development run on a loopback origin uses
local defaults, so `pnpm dev` works without setup. A production process (`pnpm build`,
`pnpm start`) is always a deployment: it refuses to start without `NEXT_PUBLIC_APP_ORIGIN`,
`BACKEND_SECRET` and `BETTER_AUTH_SECRET`, and never falls back to localhost or the development
secrets. A deployment also sets `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`.

## Two-phase checkout

`purchase()` in `src/warehouse.ts` runs in two exclusive transactions:

1. **Reserve.** Stock for every line, the district's order counter, the customer's balance and a
   `draft` order are accepted together or not at all. This is where two operators racing for the
   last units are decided: one reserves, the other re-reads stock and gets "Insufficient stock".
2. **Place.** The order's lines and the payment hand-off are written against the now-committed
   order, and the order becomes `pending`, which puts it in the delivery queue.

It is two-phase because a permission `exists` check only sees committed rows (INV-RLS-9 in the
Jazz authorization spec). The policies that prove a line or a payment belongs to an order of its
own warehouse can't see an order staged in the same transaction, so the order has to be committed
first.

The draft stores its reservation (`orders.reserved_lines`): the normalised lines and amounts the
first phase took stock and balance for. Placing and releasing work from that reservation, never
from a later request, so a resubmitted request key must ask for the same lines or it is refused.

If the authority rejects the second phase, the draft is released: its reserved stock and balance
are returned and it is cancelled. If the second phase is interrupted any other way (a lost
connection, a closed tab), the draft stays visible as "Reserved" in order status and never enters
the delivery queue. Resubmitting the same request places it, and order status offers "Place
order" and "Release" for any reserved order, so a reservation can't get stuck after its request
key is lost. Dashboard counts ignore drafts and cancelled orders.

## Permissions

`permissions.ts` answers two questions for every operational row:

- **Who may write it** ([#1899](https://github.com/garden-co/jazz/issues/1899)): the manager of
  the row's own warehouse (`warehouses.operator_id`) or an operator staffed on that warehouse
  (`warehouse_operators`). A manager can hand the warehouse only to someone already staffed on
  it, and the handover revokes the former manager's operational writes too; an operator who
  leaves loses them immediately.
- **What it may reference** ([#1898](https://github.com/garden-co/jazz/issues/1898)): a
  customer's district, an order's district and customer, a line's order and stocked item, a
  payment's customer and order, and a delivery's order all belong to the row's own warehouse
  (and district). A checkout therefore cannot combine rows from two warehouses, even for someone
  who operates both. `purchase()` checks the same thing first, to give the operator a clear error.

Stock may not go negative, and a reorder level may not exceed `REORDER_LEVEL_CAP`.

Operational reads are public in this demo so any console can observe any warehouse; the switcher
shows the other warehouse as view only.

## Reads

Every read the console subscribes to is ordered where it matters and bounded (`src/warehouse.ts`,
checked by `schema.test.ts`). Dashboard counters read at most 500 rows and show "500+" beyond
that, because Jazz has no count aggregate yet.

The stock-level report can't compare `on_hand` with `reorder_level` in a query yet
([#1864](https://github.com/garden-co/jazz/issues/1864)). It reads the indexed range
`on_hand < REORDER_LEVEL_CAP` and filters in the console; the reorder-level cap in the permissions
makes that candidate set complete.

## Tests

- `pnpm --dir examples/jamazon-warehouse test` runs `schema.test.ts` (indexes, bounded reads,
  seed consistency) and `tests/permissions` against a local Jazz server: operator staffing,
  handover and revocation, the cross-warehouse rejections, the two-operator stock race, the
  two-phase checkout (placing an interrupted draft, refusing a reused key with different lines,
  and releasing a reservation whose placement was rejected), retried checkout and delivery
  batches. It also checks that bundled modules import each other without a `.js` extension,
  which Turbopack (`next dev`, `next build`) cannot map to the `.ts` source.
- `pnpm --dir examples/jamazon-warehouse test:browser` runs the browser topology receipt: a
  duplicated and dropped checkout hand-off, reconnect, persistent reopen and ownership transfer.

`benchmarks/` duplicates the schema and query shapes in a deterministic Divan fixture;
`benchmarks/benches/walltime.rs` times 100 checkouts, a retried checkout and the pending-order
page on CodSpeed wall time, and `benchmarks/metadata.ts` documents each case for the examples
page.
