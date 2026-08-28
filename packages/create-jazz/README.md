# create-jazz

Scaffold a new [Jazz](https://jazz.tools) app from a starter template.

## Usage

```bash
npm create jazz@latest my-app
# or
pnpm create jazz my-app
# or
yarn create jazz my-app
```

If you omit the app name, you'll be prompted for one.

The CLI will:

1. Fetch the starter template into `my-app/`.
2. Resolve any `workspace:*` dependency ranges to concrete npm versions.
3. Initialise a git repository with an initial commit.
4. Install project-local Jazz guidance at `.agents/skills/jazz/` for compatible coding agents.
5. Run `install` using your detected package manager.

## Starters

The interactive picker lets you choose a framework and auth mode. You can also
skip the picker with `--starter <name>`:

| Starter                | Framework | Auth                                      |
| ---------------------- | --------- | ----------------------------------------- |
| `next-localfirst`      | Next.js   | Local-first (anonymous)                   |
| `next-hybrid`          | Next.js   | Local-first + optional BetterAuth upgrade |
| `next-betterauth`      | Next.js   | BetterAuth (email + password)             |
| `sveltekit-localfirst` | SvelteKit | Local-first (anonymous)                   |
| `sveltekit-hybrid`     | SvelteKit | Local-first + optional BetterAuth upgrade |
| `sveltekit-betterauth` | SvelteKit | BetterAuth (email + password)             |

Each starter ships a working todo-list UI with permissions, schema, and
zero-config local sync.

## React Native / Expo

`create-jazz` intentionally does not offer a React Native or Expo starter yet.
The installable `jazz-rn` native-relay package now has proven Expo development
build and bare React Native autolinking paths, but Jazz's high-level foreground
React Native client is still under restoration. A generated todo app would
therefore imply a supported application API that does not exist yet.

The canonical configuration receipt is
[`examples/todo-client-localfirst-expo`](https://github.com/garden-co/jazz/tree/main/examples/todo-client-localfirst-expo).
It demonstrates the required direct `jazz-rn` dependency, Expo plugin, and New
Architecture setting without claiming Expo Go or persistent-client support.

## Requirements

- Node.js 22.12+
- An empty target directory (the CLI refuses to scaffold into a non-empty one).

## A note on versioning

New `create-jazz` releases fetch their starter, workspace config, and package
versions from an immutable `v<create-jazz-version>` source tag in
[`garden-co/jazz`](https://github.com/garden-co/jazz). That keeps an installed
CLI and the generated app on the same release snapshot.

Releases from before immutable source snapshots retain their historical
behaviour of reading `main`. Snapshot-aware releases never fall back to `main`:
if their matching tag is unavailable, the CLI fails with an upgrade hint rather
than silently scaffolding a potentially incompatible app.
