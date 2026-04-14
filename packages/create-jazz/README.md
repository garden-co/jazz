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

Currently ships a single starter: **`next-betterauth`** — Next.js + Better Auth
with a Jazz-backed todo list gated behind a signed-in dashboard.

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
