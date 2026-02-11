# Fumadocs Documentation Setup — TODO (MVP)

Set up documentation site using Fumadocs, with all code snippets sourced from type-checked and E2E-tested example apps.

## Overview

- Documentation site built on Fumadocs
- Code snippets extracted from real example apps (not hand-written markdown blocks)
- Snippets are type-checked at build time (part of the example app's compilation)
- Snippets are covered by E2E tests — if the example breaks, docs break

## Open Questions

- Snippet extraction mechanism — inline comments marking regions, or separate snippet files?
- How to handle version drift between docs and example apps
- Which example apps serve as the primary snippet source (todo app? something richer?)
