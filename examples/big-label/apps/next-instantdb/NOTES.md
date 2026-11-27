# BigLabel on InstantDB

## Versions and hosting

- `@instantdb/react` and `@instantdb/admin` 1.0.67, `instant-cli` 1.0.67; Next.js 16.2.11, React 19.2.4, Astryx
  0.6.3 with the `garden-co/design` Jazz theme (as in the Jazz copy).
- Data: a **hosted temporary Instant app** (no account). `harness.mjs` runs `pnpm install` on a fresh checkout,
  creates the app into `.env` (git-ignored) and pushes `instant.schema.ts` and `instant.perms.ts` on every
  `seed()`. Temporary apps expire; delete `.env` and the next run makes a new one.
- Region: `api.instantdb.com` via CloudFront IAD (us-east). The Next server (sign-in, bootstrap, members and
  demo-data routes, pages) runs locally, like the Jazz copy's.

## How it maps

- **Auth**: email + password through Instant's documented custom-auth route. `app/api/auth/sign-in` and
  `sign-up` check scrypt hashes in an admin-only `credentials` namespace and mint an Instant token with
  `createToken`; the browser signs in with `signInWithToken`. The sign-in screen is unchanged. API routes
  identify the caller from the Instant token the browser sends (`Authorization: Bearer`), verified with
  `adminDb.auth.verifyToken`, instead of a session cookie.
- **Admission** follows the Jazz app route by route:
  - `POST /api/bootstrap` uses admin authority to get-or-create the profile, the sign-in email and the personal
    label with its admin membership.
  - `POST /api/members` reads as the caller to answer only admins, looks the email up with admin authority,
    then writes the membership **as the caller** (`adminDb.asUser({ token })`), so `instant.perms.ts` decides:
    only admins add members, never as admins.
  - `POST /api/demo-data` writes with admin authority, as in Jazz.
  - Browsers can't create labels or people (`create: "false"`), and `personEmails` has no client rule at all.
- **Schema**: the Jazz tables become namespaces. Every tenant row links to its organization, and relations
  (release → artist/catalogue, assignment → team/membership, release team → release/team) are links, so the
  rules check `data.ref('artist.organization.id') == data.ref('organization.id')` and the like.
- **Permissions** (`instant.perms.ts`): the Jazz rules. I checked them against the hosted app with an admin,
  an editor, a viewer and a second label's admin.
  - Refused: client-created labels, inviting an admin, inviting by a non-admin, self-promotion, role fields
    that disagree with the role, editors creating teams, viewer writes, forged or duplicate catalogue keys,
    foreign artists and teams, and deleting an artist with releases.
  - People are visible only to co-members; outsiders read nothing of another label.
- **Seed** (`seed.ts`): the Jazz seed's steps on Instant's equivalents:
  - all 48 members sign up through the app's `signUp` and run the bootstrap;
  - labels and their first admin are written with admin authority, as the demo-data route does;
  - the other members are added as `POST /api/members` does;
  - teams, catalogues, artists, team members, releases and release teams are written as the label's admin
    (`asUser`, rules apply), in batches of 200.

  About 2.5 minutes against the temporary app.

## The hard parts

- **Unique catalogue numbers.** Instant transactions can't read, so there is no equivalent of `saveRelease`'s
  serializable read of the number's holders. Each release instead carries `catalogKey` =
  `"<labelId>/<catalogNumber>"`, a **unique attribute**, and the rules require it to match the release's label
  and number on create and update. The server enforces it for every write, through the app or not, which is
  stronger than the Jazz app ("a client writing to `releases` directly could still create a duplicate").
  `saveRelease` turns the unique violation into `CatalogNumberTakenError`. The dialog's quick pre-check stays
  (a `queryOnce` instead of a local read).
- **Roles in rules.** Instant rules can't filter a linked list (`data.ref`) by role. So each membership
  carries `adminUserId` and `editorUserId` (its `userId` when the role grants it, else null), and the rules
  check `auth.id in data.ref('organization.memberships.adminUserId')`. The membership rules require the fields
  to match `role` on create and update. You can't change your own membership unless you're already its admin,
  so nobody promotes themselves.
- **Double-submitted members.** `memberKey` = `"<labelId>/<userId>"` is unique, so a second add fails on it
  (reported as "already a member"). This replaces the serializable read-then-insert.
- **Inviting someone you can't see.** By default Instant checks that you can *view* a row you link to. An
  admin can't see a person who isn't a member yet, so memberships have an explicit link rule
  (`link: { person: "linkedData.userId == data.userId" }`), which replaces that check. It applies to new rows
  too.
- **Bootstrap races.** `people.userId`, `organizations.slug` and `memberships.memberKey` are unique, so two
  concurrent bootstraps can't both commit. The loser's transaction fails and its retry reads the winner's rows.
- **Deleting an artist with releases** is refused by the delete rule (`data.ref('releases.id') == []`),
  evaluated by the server, instead of a serializable read.
- **Cascades.** Deleting a release, team or member removes its assignments through `onDelete: "cascade"`
  links, in the same transaction, instead of reading and deleting them.

## Approximations

- **Paging and sorting**: Instant orders by one indexed attribute, so the Jazz app's `orderBy(sort).orderBy(id)`
  loses its `id` tie-break. Release dates are unique per label in the fixture, so the default Releases order
  (and every harness scenario) is unaffected. Ties in other sort keys (title, status, genre) come back in
  Instant's order, which may not be stable across pages.
- **Search**: `searchKey` `contains` becomes `$like: "%term%"` on the same lower-cased, indexed attribute.
- **Counts**: the Overview's bounded reads (`limit 5000`, `fields: ["id"]`) are kept as they are.
- `src/scenarios.ts` (an upstream receipt that only the Jazz tests use) is not copied, so the Jazz `server`
  LOC count includes 38 lines that have no counterpart here.
- Writes report a server refusal through the `transact` promise, which replaces the global-tier wait.

## Harness notes

- `signIn` uses the app's sign-in form, which loads the app. The Jazz hooks sign in through an API without
  loading a page, so their timed first open is cold. To match, `signIn` then goes to a same-origin page that
  loads no app code (`/api/auth/sign-in`, a 405) and:
  - clears Instant's cached query results (the `querySubs` IndexedDB store), keeping the session;
  - clears the app's localStorage (which also means the first open runs the bootstrap request, as in Jazz);
  - clears the context's HTTP cache over CDP, which is per browser context.

  It ends on `about:blank`, so the scenario's `goto("/#/releases")` is a full page load that runs its init
  script (the one that preselects label 0).
