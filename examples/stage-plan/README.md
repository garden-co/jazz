# StagePlan

A crew preparing shows. Each show (a gig with a venue, date and doors time)
has a stage-prep board: load-in, line check, backline, soundcheck, grouped in
To do, In progress, Done and Blocked, assigned to crew, with comments and an
activity feed. There is also a personal checklist for show day.

StagePlan is the Jazz example for two things:

- **Local-first basics** (what the todo examples teach): writes apply
  instantly, live queries follow every device, edits made offline sync when
  you reconnect, and row-level permissions decide who sees what.
- **A permissioned project tracker** (the W1 "team task board" workload): a
  board per show, task details, comments, an activity log and a crew with
  roles.

## Apps

| App                                              | Stack                                                         |
| ------------------------------------------------ | ------------------------------------------------------------- |
| [`apps/react-localfirst`](apps/react-localfirst) | React, Vite, local-first accounts, Astryx with the Jazz theme |

## Run it

From the repository root, after `pnpm install` and `pnpm build:ci`:

```sh
cd examples/stage-plan/apps/react-localfirst
pnpm dev
```

`pnpm dev` starts a local Jazz server through the Vite plugin. The first time
you open the app, it creates your crew profile and a demo show,
"The Late Lanterns: album launch", with the same eight tasks every time.
If the server rejects part of the demo while the tab is open, a banner offers
**Save the rest**. If that happens after the tab has closed, the next time
the chief opens the show the app restores their membership and the invite,
but not the board: the missing tasks stay missing.

To see sync and permissions with two people, open the app in a second browser
profile (or a private window). On the demo show, open **Crew**, copy the
invite link and open it in the other profile. Moves, comments and assignments
now show up on both sides as they happen. Turn **Sync** off in the top bar
to edit offline; the changes reach the other profile when you turn it back on.

## Who can do what

Permissions live in [`permissions.ts`](apps/react-localfirst/permissions.ts)
and are enforced by the server.

| Person                             | Show and board | Tasks                                 | Invite link and crew list                       |
| ---------------------------------- | -------------- | ------------------------------------- | ----------------------------------------------- |
| Crew chief (created the show)      | Sees and edits | Adds, edits, moves, deletes any       | Reads the invite, makes a new one, removes crew |
| Crew (joined with the invite link) | Sees           | Adds, edits, moves; deletes their own | Sees the crew list; can leave                   |
| Anyone else                        | Nothing        | Nothing                               | Nothing                                         |

- Invite codes sit in their own `showInvites` table that only the chief can
  read. Joining inserts a `showCrew` row carrying the code; the server accepts
  it only if a matching invite exists. Membership rows are visible to the
  whole crew, so once the server accepts the join the app clears the code
  from the row; the only change a member may make to their membership is
  clearing it. Until then, other crew could read the code. Making a new link
  deletes the old code, so the old link stops working (people who already
  joined stay on the crew).
- Tasks are assigned to nobody or to someone on the show's crew.
- Comments and activity are written as your own crew profile; nobody can post
  as someone else. The activity log is append-only. Authors may delete their
  own comments, though the app doesn't offer that yet.
- The server doesn't check that an activity entry's task belongs to its show.
  A policy could now look up the task, since a transaction's policy checks
  see the rows it wrote earlier
  ([#3755](https://github.com/garden-co/jazz/issues/3755)), but the example
  doesn't do that yet.
- Checklist items are private to their owner.
- Crew profiles (display names) are readable by every signed-in account, like
  the chat examples' profiles.

## Identity

Like the todo examples, StagePlan uses a local-first account: no sign-up, the
browser profile holds the account. That keeps the example small; see the
[auth docs](https://jazz.tools/docs/auth/authentication) to add a provider.

## Schema and the W1 workload

The core tables map one to one onto the W1 benchmark schema, so the workload
can later run against this app's schema:

| W1         | StagePlan  | Columns                                                                                            |
| ---------- | ---------- | -------------------------------------------------------------------------------------------------- |
| `users`    | `crew`     | `name` (+ `account`)                                                                               |
| `projects` | `shows`    | `name` (+ `venue`, `date`, `doors`, `chiefAccount`)                                                |
| `tasks`    | `tasks`    | `showId`, `title`, `status`, `assigneeId`; `updated_at` is Jazz's `$updatedAt` (+ `notes`, `rank`) |
| `comments` | `comments` | `taskId`, `authorId`, `body`; `created_at` is `$createdAt`                                         |
| `activity` | `activity` | `showId`, `taskId`, `actorId`, `kind`; `created_at` is `$createdAt` (+ `detail`)                   |

`showCrew` (memberships), `showInvites` and `checklistItems` are the extra
tables a real app needs. Benchmarks for StagePlan belong in
`examples/stage-plan/benchmarks`.

## Tests

```sh
cd examples/stage-plan/apps/react-localfirst
pnpm test:permissions   # chief, crew and outsider against a local Jazz server
pnpm test:browser       # the board flow in Chromium, synced to a second client
pnpm typecheck
```
