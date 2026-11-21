# StagePlan on InstantDB

## Versions and hosting

- `@instantdb/react`, `@instantdb/admin` and `instant-cli` 1.0.67; React 19.2.4, Vite 8.0.1 (as in the Jazz copy).
- Backend: a **hosted temporary Instant app** (no account), created by `instant-cli init-without-files --temp`.
  `harness.mjs` creates one into `.env` (git-ignored) the first time and pushes `instant.schema.ts` and
  `instant.perms.ts` on every `seed()`. Temporary apps expire (the docs say after 24 h to 2 weeks); delete
  `.env` and the next run makes a new one.
- Region: `api.instantdb.com` answers through CloudFront (`x-amz-cf-pop: IAD…`, so us-east). The harness's
  `rtt` is measured against `https://api.instantdb.com/`. The Jazz app runs its backend locally, so
  Instant numbers include a network hop the Jazz numbers don't (`rtt_ms` is about 8 ms from this sandbox).

## Running

```sh
pnpm install
node ../../../../dev/app-comparisons/harness/run.mjs stage-plan/instantdb      # from the repo root: seed, build, serve, measure
pnpm seed                                             # reset the app and write the fixture (needs .env)
```

## How it maps

- **Schema** (`instant.schema.ts`): the Jazz tables become Instant namespaces. Rows that belong to a show
  or task are **links** (`tasks.show`, `showCrew.show`, `comments.task`, …), so permission rules can follow
  them with `data.ref(...)`. `assigneeId` stays an attribute (as in Jazz), so the update rule can check the
  new value against the crew. Jazz's `$createdAt`/`$updatedAt`/`$createdBy` become explicit
  `createdAt`/`updatedAt`/`creatorAccount` attributes set by the client (Instant has no readable server
  timestamps); `creatorAccount` is checked server-side on create and can't change.
- **Permissions** (`instant.perms.ts`): same "who can do what" as `stage-plan/jazz/permissions.ts`; default
  deny for every namespace and for new attributes. Checked against the hosted app with users A (chief),
  B (joiner) and C (outsider): wrong code, joining as chief, outsider reads and writes, assigning to
  non-crew, deleting someone else's task, comments as someone else, and acting after leaving are all
  rejected.
- **Invite links**: the joiner sends the code as a `ruleParam` (Instant's documented share-link pattern).
  The membership's create rule checks it against the show's private invites, and the show's `view` rule
  lets a holder of a valid code see the show. That second part is needed because **Instant checks the
  linked row's `view` rule when you link to it**, and a joiner isn't crew yet. Unlike the Jazz app, the code
  is never stored on the membership, so there's no "clear the code" step afterwards.
- **Auth**: Instant guest users (`db.auth.signInAsGuest()`), signed in automatically when there is no
  session, like Jazz device accounts. The crew profile's id is the user's id.
- **Seed** (`seed.ts`): admin SDK, as the fixture chief (`chiefAccount` is the same constant uuid as in the
  Jazz seed; there's no `$users` row for it). One transaction per show (~113 chunks), about 9 s for all 30
  shows. `seed()` first deletes every row in the app.

## Approximations

- **Notes merge**: Jazz merges concurrent edits of a task's notes (`merge("text")`). Instant has no text
  merge for plain attributes, so the last write wins.
- **Sync switch**: Instant has no public API to pause syncing. The switch shows whether Instant is
  connected (`useConnectionStatus`) and is disabled. Same DOM and label, but it can't be flipped.
- **Repair paths dropped**: the Jazz app repairs a chief membership or first invite that the server
  rejected (`ensureChiefSetup`, stable v5 ids) and resumes a partly saved demo show. Instant transactions
  are atomic, so a show is saved or rolled back as a whole, and the "Save the rest" button just writes the
  demo show again.
- **`canDelete`**: Jazz asks the policy engine (`db.canDelete`). Instant has no such API, so the dialog
  computes it from the same rule (the chief, or the task's creator). The server still enforces it.
- **Checklist filter**: Jazz `contains` → Instant `$like: %text%` (case-sensitive, needs an indexed
  attribute).
- **Offline first load**: Jazz opens offline from a cold start. Instant needs the server for the guest
  sign-in and the profile check (`queryOnce`), then caches queries in IndexedDB for later loads.

## Gotchas found

- Instant checks links against the *other* side's rules: by default, linking requires `update` on your
  entity and `view` on the linked one (or explicit `allow.link` rules). This is in the server source
  (`permissioned_transaction.clj`) and the `InstantRules` types, not in the permissions docs page.
- Temporary apps rate-limit bursts of admin `asUser` calls (about 20 per second in the permission tests).
