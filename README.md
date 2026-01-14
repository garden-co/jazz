# Jazz2 (Groove)

A distributed database that syncs across frontend, backend, and cloud. Data feels like local reactive state but syncs automatically.

**Status**: Active development, not production-ready.

## What is this?

Jazz2 is a rewrite of [Jazz](https://jazz.tools) with a different trust model:
- **Trusted sync server** (E2EE opt-in, not default)
- **SQL as the query interface**
- **Git-style history** per object
- **ReBAC permissions** (relationship-based access control)

Core written in Rust, TypeScript bindings via WASM.

## Quick Start

```bash
# Rust tests
cd crates && cargo test

# Build WASM
cd crates/groove-wasm && wasm-pack build --target web

# TS packages
pnpm install && pnpm build

# Run demo
cd examples/demo-app && npm run dev
```

## Structure

```
crates/           Rust workspace (groove, groove-wasm, groove-server, groove-cli)
packages/         TypeScript packages (@jazz/client, @jazz/react, @jazz/schema)
examples/         Demo apps and documentation examples
docs/             Documentation site (Fumadocs)
```

See [CLAUDE.md](CLAUDE.md) for detailed project structure and development guidelines.

## Documentation

- **Architecture**: [docs/content/docs/internals/architecture.mdx](docs/content/docs/internals/architecture.mdx)
- **Deep dives**: [docs/content/docs/internals/](docs/content/docs/internals/)
- **Full docs site**: Run `cd docs && npm run dev`

## Claude-First Development

This repository is designed for AI-assisted development. The [CLAUDE.md](CLAUDE.md) file provides:
- Project structure and entry points
- Build commands
- Coding guidelines
- Links to specs

Using Claude Code or similar AI tools for development is encouraged.

## License

[License details here]
