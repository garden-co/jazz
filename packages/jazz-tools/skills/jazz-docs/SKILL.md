---
name: jazz-docs
description: Find and use the current official Jazz documentation. Use when the user asks for Jazz documentation, links, API references, currently supported features, or a version-agnostic Jazz answer and there is no installed project, package, or source checkout to inspect. Do not load for ordinary implementation work when the project's installed Jazz version, public types, and a specialised Jazz skill already answer the question.
---

# Jazz Documentation

Use the live documentation to resolve questions that cannot be answered from the target project's
installed Jazz version. Keep implementation advice compatible with the version the project actually
uses.

## Choose the authority

1. If a project is available, read its installed Jazz version, public type declarations, existing
   code, and tests first.
2. Treat those local sources as authoritative for version-specific implementation details.
3. Use the live documentation when the user explicitly wants current documentation, when no project
   is available, or when local sources do not answer a conceptual question.
4. If the live documentation differs from the installed API, follow the installed API for the
   project and explain the version difference.

## Find the relevant page

1. Fetch `https://jazz.tools/llms.txt`.
2. Select the page whose title and description match the question.
3. Fetch that page by appending `.mdx` to its documentation URL. For example, fetch
   `https://jazz.tools/docs/schemas/defining-tables.mdx` for the page at
   `/docs/schemas/defining-tables`.
4. Fetch only the pages needed for the question. Do not load the complete documentation set.

## Answer from the evidence

- Link to the normal human-readable documentation page in the answer, not only its `.mdx` form.
- Distinguish documented current behaviour from an inference or a project-specific observation.
- Do not guess documentation paths; derive them from the index.
- Do not substitute APIs remembered from another Jazz version or another database.
- Use the specialised Jazz implementation skill when the task moves from research into code changes.
