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

| Starter                | Framework                       | Auth                                             |
| ---------------------- | ------------------------------- | ------------------------------------------------ |
| `next-localfirst`      | Next.js                         | Local-first (anonymous)                          |
| `next-hybrid`          | Next.js                         | Local-first + optional BetterAuth upgrade        |
| `next-betterauth`      | Next.js                         | BetterAuth (email + password)                    |
| `react-localfirst`     | React (Vite)                    | Local-first (anonymous)                          |
| `react-hybrid`         | React (Vite)                    | Local-first + optional BetterAuth upgrade        |
| `react-betterauth`     | React (Vite)                    | BetterAuth (email + password)                    |
| `sveltekit-localfirst` | SvelteKit                       | Local-first (anonymous)                          |
| `sveltekit-hybrid`     | SvelteKit                       | Local-first + optional BetterAuth upgrade        |
| `sveltekit-betterauth` | SvelteKit                       | BetterAuth (email + password)                    |
| `ts-localfirst`        | TypeScript (Vite, no framework) | Local-first (anonymous)                          |
| `ts-hybrid`            | TypeScript (Vite, no framework) | Local-first + optional BetterAuth upgrade        |
| `ts-betterauth`        | TypeScript (Vite, no framework) | BetterAuth (email + password)                    |
| `ts-effect-localfirst` | TypeScript + Effect             | Local-first (anonymous); Effect client           |
| `ts-effect-betterauth` | TypeScript + Effect             | BetterAuth; Effect client and Effect HTTP server |

Each starter ships a working todo-list UI with permissions, schema, and
zero-config local sync.

### Hosting

The picker asks where the app syncs. When you skip the picker (with
`--starter`, or when output is not a terminal), set this with
`--hosting <value>` instead; the interactive picker ignores the flag:

| Value        | Behaviour                                                    |
| ------------ | ------------------------------------------------------------ |
| `hosted`     | Provision a Jazz Cloud app at scaffold time                  |
| `selfhosted` | Skip provisioning; the dev plugin starts a local Jazz server |

Without `--hosting`, `--starter` defaults to `selfhosted`, and a
non-interactive run without `--starter` defaults to `hosted`.

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
