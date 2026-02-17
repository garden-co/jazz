# Type-Aware oxlint Rules — TODO (Launch)

Enable type-aware oxlint features (currently in alpha) for stricter TypeScript linting.

## Overview

oxlint is adding type-aware lint rules that can catch more issues than syntactic-only analysis. Once stable enough, enable these for the TypeScript packages:

- Type-checked no-unused-vars, no-floating-promises, etc.
- Stricter than current oxlint config without the cost of full tsc-based eslint
- Track oxlint alpha/beta releases for type-aware feature graduation

## Open Questions

- When will type-aware rules hit stable? Track upstream.
- Performance impact on CI — type-aware analysis is slower than syntactic
- Which rules to enable first — start with high-value, low-noise rules
