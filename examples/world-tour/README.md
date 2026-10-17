# Jazz World Tour example

A band plans its world tour on a dot-art globe; fans follow the confirmed dates. Built with Vue + Vite and the Jazz Vite plugin. The globe is a custom 2D canvas projection: illustrative, not cartographic.

## Getting started

```bash
pnpm dev
```

`pnpm dev` starts the Jazz dev server and the Vite dev server together via the Jazz Vite plugin. The first visitor to an empty server gets the seeded demo tour and owns the band.

## Who sees what

Every visitor has a local-first account. Band membership, enforced in [`permissions.ts`](./permissions.ts), decides the rest:

- **Public visitors** see the band and its confirmed stops. Tentative and cancelled stops, private notes, invites and the member list never reach them.
- **Members** see and edit every stop, rename the band and add venues. They join with the owner's invite link (`#/bands/<id>/join/<code>`); nobody can add themselves to a band without the current code.
- **The owner** manages the invite link and removes members. Removing a member also resets the link, so a revoked member can't rejoin with the old one.
- **Venues** are public places. Each band adds its own and its members manage them, so no other band can move or delete a venue this band's stops use.

## Demo data

[`src/fixture.ts`](./src/fixture.ts) builds the demo tour from a seedable PRNG with a fixed default seed, so every fresh install shows the same twelve shows over the next three weeks.

## Tests

```bash
pnpm test:unit      # fixture determinism and permissions (member, public, outsider, revoked)
pnpm test:browser   # Vue bindings against a local Jazz server
pnpm walkthrough:shots
```

## Benchmark variant

`benchmarks/` is a self-contained native workload variant. It duplicates the
public schedule and venue shapes needed to measure the app's two browse paths:
the member calendar and the confirmed-only public calendar. Both are ordered,
bounded three-week itinerary reads with their venue relation included.
It does not import frontend code or cover the app's permissions, and its
shapes predate the app's `ownerId` and `stopNotes` columns.

```bash
cargo test -p jazz-example-world-tour-benchmark
cargo bench -p jazz-example-world-tour-benchmark --bench queries
```
