# Jamazon

Jamazon is a music-instrument storefront built on Jazz. It is the shop side of the fictional
company whose warehouse runs [Jamazon Warehouse](../jamazon-warehouse): the two share a brand and
a synthetic catalogue (SKUs `JAM-001` to `JAM-003` are the strings, cable and picks the warehouse
fixtures use) but keep separate schemas.

The canonical app is [`apps/nextjs-betterauth`](apps/nextjs-betterauth): Next.js, Better Auth and
the Jazz design system.

## What it shows

- **A public catalogue.** Categories, a product grid, product pages with generated illustrations,
  specifications and live stock badges. Search is a live Jazz query over the local copy of the
  catalogue, so it narrows on every keystroke and keeps working offline.
- **An offline-first cart.** Every visitor gets a local-first Jazz account on first load, so the
  cart works before sign-in and without a connection. Cart edits are ordinary local writes that
  sync to the shopper's other devices. Each (cart, product) pair has one row with a
  deterministic id: two devices that add the same product converge on one line, and if they set
  different quantities the later edit wins. Lines are zeroed rather than deleted, so removing
  and re-adding a product addresses the same row everywhere.
- **Guest carts that follow you.** Creating an account links the Better Auth identity to the
  guest's Jazz account (`linkJWT`), so the cart stays put. Signing in to an existing account
  switches accounts, and the app claims the guest cart's lines into the account's cart, keeping
  the larger quantity where both have the product.
- **An idempotent checkout.** Shipping details live on the cart row (they sync too). Reaching the
  review step mints an idempotency key; "Place order" sends only that key. The backend reads the
  cart, prices and stock at the authority in one exclusive transaction, and derives the order id
  from (account, key), so double clicks, network retries and racing requests all return the same
  order and never decrement stock twice.
- **Payments behind an interface.** `PaymentProvider` has two implementations, chosen by
  `PAYMENT_PROVIDER`:
  - `sandbox`, for local runs and tests, is labelled on every payment screen. It moves no money
    and collects no card details: the shopper picks "Approve" or "Decline".
  - `stripe` runs Stripe in test mode. The server creates a PaymentIntent with an
    `Idempotency-Key` derived from the order; the browser confirms it with Stripe Elements; the
    server reads it back from Stripe and records the result.

  Either way the browser never marks an order paid. Results are written back idempotently: a
  succeeded payment is final, and each status appears once on the timeline.

- **Backend authority.** A backend Jazz client seeds the catalogue, places orders, records
  payments and runs a fulfilment worker that subscribes to paid orders and ships them after a
  short packing delay. Shoppers watch the order timeline (placed, paid, shipped) advance through
  the same live query.
- **Permissions in `permissions.ts`.** The catalogue and stock are public and read-only. Carts
  and their lines belong to one account, quantities stay in range and a line cannot be moved into
  another cart. Orders, order lines, events and payments are readable by their shopper and
  written only by the backend.

## Run it

```bash
pnpm install
pnpm --filter jamazon-nextjs-betterauth dev
```

Open <http://127.0.0.1:3000>. `pnpm dev` starts a local Jazz server alongside Next.js. The
checked-in local defaults need no configuration and select the sandbox payment provider; see
[`.env.example`](apps/nextjs-betterauth/.env.example) for everything else.

To try Stripe test mode, set `PAYMENT_PROVIDER=stripe`, `STRIPE_SECRET_KEY=sk_test_…` and
`NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY=pk_test_…`. Live keys are refused.

A short tour:

1. Browse, search and add a few things to the cart. Open the app in a second window: the cart
   is there too. Go offline in devtools and keep editing; reconnect and it syncs.
2. Check out: enter an address, review, then create an account when asked. The cart comes along.
3. Place the order, pay with the sandbox (try "Decline" first), and watch the timeline move to
   shipped a few seconds later.

## Tests

```bash
pnpm --filter jamazon-nextjs-betterauth test
```

Runs against a local Jazz server started by `createPolicyTestApp`:

- `tests/permissions.test.ts`: the catalogue is public and read-only; carts are private and
  lines cannot move between them; orders and payments are readable only by their shopper and
  never writable by clients.
- `tests/checkout.test.ts`: racing and repeated checkouts make one order and decrement stock
  once; payments are created once per order with one idempotency key; a decline then an
  approval, duplicate and late reports settle to one paid order; racing fulfilment workers ship
  once; stale or foreign idempotency keys are refused; stock is never oversold.

## Layout

| Path                          | What lives there                                               |
| ----------------------------- | -------------------------------------------------------------- |
| `schema.ts`, `permissions.ts` | Tables and row-level policies                                  |
| `src/catalogue/catalogue.ts`  | The deterministic synthetic catalogue                          |
| `src/lib/ids.ts`              | Deterministic (version 5) row ids                              |
| `src/store/cart.ts`           | Cart hook, merge rules and guest cart claim                    |
| `src/server/orders.ts`        | Checkout, payment and shipping workflow (backend authority)    |
| `src/server/payments/`        | `PaymentProvider`, the sandbox and Stripe test mode            |
| `src/server/fulfilment.ts`    | The worker that ships paid orders                              |
| `src/components/`             | Storefront UI built from Astryx components with the Jazz theme |
