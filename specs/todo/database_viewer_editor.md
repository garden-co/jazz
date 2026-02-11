# Database Viewer & Editor — TODO

Visual tool for inspecting and editing jazz2 database contents.

## Overview

A web-based UI (embeddable or standalone) for:

- Browsing tables, schemas, and rows
- Editing data inline
- Viewing object history and edit provenance
- Inspecting sync state (which peers have which data)
- Schema migration status and lens chains
- Query playground (run SQL, see results live with reactivity)

## Open Questions

- Standalone app vs. embedded in the developer dashboard?
- Read-only mode vs. full editing (editing through the normal sync path)?
- How to visualize merge conflicts and resolution?
- Branch visualization (tree/graph of branches)?
- Built with jazz2 React bindings (dogfooding)?
