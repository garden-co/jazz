# Fumadocs Documentation Setup — TODO (MVP)

Set up a Fumadocs documentation site with environment-switchable examples (TypeScript + Rust), where every snippet comes from real example app files that are buildable and testable.

## Goals

- Launch a docs site built on Fumadocs.
- Cover both supported environments on core concepts:
  - TypeScript as the default/primary view.
  - Rust as the secondary tab on all pages and concepts (except React-only hooks).
- Keep docs snippets tied to real example code, not hand-written docs-only blocks.
- Ensure docs correctness is enforced in CI via example builds/tests.

## MVP Scope

### Documentation Information Architecture

- `Setup`
  - Hosted DB setup (current manual provisioning workflow).
  - Self-hosted DB server via CLI.
  - Context setup for:
    - React client
    - TS client
    - TS backend
    - Rust backend
  - Backend identity pattern:
    - Client-side use enforces permissions for the local user context.
    - Backend use must extract requester JWT and create a user-scoped client/session before reads/writes.
- `Schemas`
  - Table definitions in TS DSL (default tab) and SQL (alternate tab, especially for Rust-only projects).
  - Datatypes and references.
  - Jazz CLI for TS client codegen.
- `Permissions` (top-level page)
  - Fast RLS model (ReBAC).
  - Simple policies.
  - `INHERITS` policies.
  - Callout back to setup page for backend request-scoping pattern.
- `Evolution & Migrations` (top-level page)
  - Explicit "skip this for first app" guidance.
  - Traditional migrations vs Jazz read/write-time lenses.
  - CLI workflow to generate migration stubs.
  - Examples of editing generated migration stubs (TS + SQL).
- `Reading Data`
  - One-shot queries.
  - Subscriptions.
  - Reactive hooks (React-only section for now).
  - Filters, sorts, pagination, includes.
  - Settled tier semantics.
- `Writing Data`
  - Insert, update, delete.
  - Write-ack tier semantics.

### Snippet Sourcing

- Use Fumadocs native include/region extraction (`<include>`) with region markers in source files.
- All snippets must come from committed example app files.
- Include snippets from generated files when relevant (for example generated migration stubs), plus snippets showing intentional edits to those generated stubs.
- If a concept needs multiple step states, maintain versioned snippet-source example folders; each state must remain type-checkable and testable.

### Environment Tabs

- Use per-code-example tabs initially.
- Add tab synchronization across examples in-page as MVP behavior (shared tab selection).
- Rust is always present as secondary tab for parity, except where a concept is framework-specific (for example React hooks).

### Example Source Baseline

- Start with current todo examples as primary sources:
  - `examples/todo-client-localfirst-react`
  - `examples/todo-client-localfirst-ts`
  - `examples/todo-server-ts`
  - `examples/todo-server-rs`

### Config Naming Standardization

- Normalize app ID configuration naming across docs and examples to `JAZZ_APP_ID`.
- Update current examples to align with this naming so docs and runnable code match.

## Non-Goals (MVP)

- A standalone quickstart page (defer until adopter data clarifies best first-run path).
- Under-the-hood internals page (defer).
- Multi-version docs support.

## Design Notes

- Hosted setup should explicitly document current multi-tenant onboarding:
  - App IDs are manually provisioned today.
  - Adopters should request provisioning in the `jazz2-adopters` Discord channel.
  - Intake fields for now: app name and JWKS endpoint.
- Keep backend permission-context setup centralized in `Setup`; other pages should reference it rather than duplicate logic.

## Acceptance Criteria

- Fumadocs site exists in the monorepo and builds in CI.
- Docs include the page set listed in MVP scope (`Setup`, `Schemas`, `Permissions`, `Evolution & Migrations`, `Reading Data`, `Writing Data`).
- `Setup` docs cover hosted and self-hosted paths, and include backend JWT extraction + user-scoped client/session patterns in TS and Rust.
- Snippets are sourced from committed example files only, including generated migration stubs and edited-stub examples.
- TS is the default tab and Rust secondary tab across all pages/concepts, with explicit React-only scope note for hooks.
- Schema docs provide TS DSL by default and SQL as an alternate view.
- In-page tab sync works across examples.
- Example apps and docs use `JAZZ_APP_ID` consistently.
- CI fails on docs build regressions and on snippet-source example build/test regressions.

## Open Questions

- Exact repo location for docs app and content structure.
- Conventions for naming/versioning multi-state snippet-source example folders.
- Minimum CI matrix for validating all snippet-source example states while keeping runtime reasonable.
