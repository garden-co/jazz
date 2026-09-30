# BandBook

BandBook is a Notion-style notebook for running a band that also tracks
issues. Setlists, lyrics, tour notes and the band's to-do list live in one tree
of pages. Each page holds an ordered, nestable list of blocks, and one page is
a database of issues shown as a table and a board.

It is the catalogue app for three Jazz patterns:

- **Deeply nested data.** Pages nest to any depth the sidebar allows, and
  blocks nest inside blocks. Sibling pages keep their real `$createdAt` order;
  blocks use fractional positions so inserting between two blocks never
  rewrites their neighbours.
- **Page-scoped roles.** A band has owners, band members (edit everything) and
  crew (read everything). On top of that, a grant on one page reaches every
  page and block below it, so a guest collaborator can edit one song and see
  nothing else. All of it is enforced by row policies in `permissions.ts`,
  never by hiding buttons; the UI asks Jazz's permission advice
  (`db.canUpdate` and friends) which controls to show.
- **A database inside the document tool.** Issues are ordinary pages under an
  issues page, with a row of properties (status, assignee, priority, labels).

## Run it

```sh
pnpm install
pnpm --filter band-book-nextjs-betterauth dev
```

Open <http://127.0.0.1:3000>, create an account, and BandBook sets up a demo
band for you: a setlist, two songs with lyrics and arrangement notes, tour
notes for two venues, and four issues.

To try sharing, open a song, choose **Share**, create a **Can edit** link and
open it in a private window with a second account. That account sees only the
song and the pages inside it, under **Shared with you**.

`apps/nextjs-betterauth/.env.example` lists the settings. The checked-in
defaults are for local development only. A production build or server
(`NODE_ENV=production`) never uses them: it refuses to start unless
`NEXT_PUBLIC_APP_ORIGIN`, `NEXT_PUBLIC_JAZZ_APP_ID`,
`NEXT_PUBLIC_JAZZ_SERVER_URL`, `BACKEND_SECRET` and `BETTER_AUTH_SECRET` are
all set. Any deployment whose app id, origin or server URL differ from the
defaults needs the two secrets as well.

## How it fits together

| Piece                                                                                 | Where                                                     |
| ------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Schema: workspaces, members, pages, page grants, blocks, attachments, issues, invites | `apps/nextjs-betterauth/schema.ts`                        |
| Every access rule                                                                     | `apps/nextjs-betterauth/permissions.ts`                   |
| Demo band content                                                                     | `apps/nextjs-betterauth/src/lib/seed.ts`                  |
| First-open bootstrap (server)                                                         | `src/lib/bootstrap.ts`, `app/api/bootstrap/route.ts`      |
| Invite redemption (server)                                                            | `src/lib/invites.ts`, `app/api/invites/redeem/route.ts`   |
| UI                                                                                    | `src/components/` (Astryx components with the Jazz theme) |

**One auth model.** Better Auth owns the browser session and signs an ES256
JWT. Jazz maps the JWT's issuer and subject to an account; every membership,
grant and assignee stores that account id. The same subject from a different
issuer is a different person. In the browser, `JazzProvider` takes
`auth={betterAuth(authClient)}`: it follows the Better Auth session, logs in or
registers the account atomically, and shows the sign-in form while signed out.
Server routes verify the same JWT, sent as a bearer, against Better Auth's JWKS
and the account registry.

**Atomic writes.** Anything that spans rows commits as one transaction: an
issue's page with its properties, a block with its children, and a page with
its whole subtree, including the grants and invite links that point into it, so
a link to a deleted page stops working.

**Retry-safe bootstrap.** The demo band is written in one exclusive
transaction with ids derived from the account, so a retry, a double click or
two tabs opening at once either finds the finished band or writes all of it.
Rows get creation times a millisecond apart in tree order, which is what the
sidebar's `$createdAt` ordering shows.

**Inherited access.** A page is readable when you have a band role, a grant on
the page, or read access to its parent (`allowedTo.read("parent")`, up to eight
levels). Editing works the same way through `allowedTo.update("parent")`.
Blocks, attachments and issues inherit from their page. Every child row also
carries its `workspaceId`, and policies check it matches the parent's, so no
write can graft a page, block or grant from one band onto another.

**Invites are capabilities.** Only owners and band members can read or create
invite tokens. The invitee's server route redeems a token with backend
authority: it adds a guest (or band) membership and the page grant the invite
describes, never lowers access someone already has, and fails once the invite
is revoked.

**Large values.** Images and files are streamed into a `bytes` column with
`db.insertStreaming`, so an upload is never held in one buffer. Page queries
select attachment metadata only; images load their bytes when shown, and file
downloads read the value in 512 KB pages with typed range selections
(`select({ bytes: { from, to } })`).

**Collaborative text.** Typing sends a minimal splice with
`update(..., { applyDiffs })` instead of the whole string, so two people typing
in the same block merge rather than overwrite each other.

## Tests

```sh
pnpm --filter band-book-nextjs-betterauth test        # policies, bootstrap, invites, helpers
pnpm --filter band-book-nextjs-betterauth typecheck
```

`tests/permissions/permissions.test.ts` runs the real policies against a local
Jazz authority: inherited grants three levels deep, revocation, who may share,
viewer denial (band-wide and page-scoped), cross-band isolation including
grafting attempts, issuer isolation, issue/database consistency and real
`$createdAt` ordering. `tests/permissions/bootstrap.test.ts` covers the
bootstrap's idempotency under retries and races, and invite redemption.

Benchmarks for BandBook are added in #3730.

## Known gaps

- Drafts and suggested edits are not implemented yet. Jazz's documented branch
  views (`branchBy`, `{ branch, base }` reads) are the intended foundation.
- Moving a page under one of its own descendants is prevented by the UI, not by
  a policy: policies cannot yet express an acyclic parent reference
  (garden-co/jazz#3743).
- Which controls appear comes from Jazz's permission advice (`db.canUpdate`
  and friends), not from a copy of the rules. Advice for moving a page under a
  page that has no subpages yet answers "denied" (garden-co/jazz#3556), so such
  pages are missing from the move dialog until that is fixed.
- Block order uses floating-point positions; after about fifty inserts at the
  same spot two blocks compare equal and fall back to creation order.
