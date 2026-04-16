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
4. Run `install` using your detected package manager.

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

## Requirements

- Node.js 20+
- An empty target directory (the CLI refuses to scaffold into a non-empty one).

## A note on versioning

Every version of `create-jazz` fetches the starter, the workspace config, and
package versions from the `main` branch of
[`garden-co/jazz2`](https://github.com/garden-co/jazz2) at scaffold time —
regardless of which CLI version you install. In other words, `npm create
jazz@0.0.1` and `npm create jazz@latest` will produce the same output on any
given day, and that output tracks whatever is on `main` right now.
