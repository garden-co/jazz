---
name: issues
description: Capture, inspect, assign, and update Jazz ideas and issues through the repo-local skill-issues CLI.
---

# Issues

Jazz ideas and issues are stored in Jazz Cloud-backed skill state. The CLI is the source of truth.

Always run commands from the repository root and delegate operations to:

```bash
pnpm --filter skill-issues cli ...
```

Do not edit `todo/` Markdown files directly. Do not create, rename, or delete Markdown source files as a fallback. `todo/` is generated/exported state only.

If a command fails, report the failure and stop. Do not fall back to Markdown capture, and do not claim the operation succeeded unless the CLI verifies it.

## Auth

Initialize local-first auth before first use:

```bash
pnpm --filter skill-issues cli auth init
```

Connect GitHub identity when needed:

```bash
pnpm --filter skill-issues cli auth github
```

## Capture

Add an issue for a bug or focused problem:

```bash
pnpm --filter skill-issues cli add issue <slug> --title "<title>" --description "<what>"
```

Add an idea for a feature or design direction:

```bash
pnpm --filter skill-issues cli add idea <slug> --title "<title>" --description "<what>"
```

Use kebab-case slugs. Keep capture factual and brief unless the user asks for shaping.

## Inspect

List items:

```bash
pnpm --filter skill-issues cli list
```

Filter by kind or status:

```bash
pnpm --filter skill-issues cli list --kind issue
pnpm --filter skill-issues cli list --kind idea
pnpm --filter skill-issues cli list --status open
pnpm --filter skill-issues cli list --status in_progress
pnpm --filter skill-issues cli list --status done
```

Show one item:

```bash
pnpm --filter skill-issues cli show <slug>
```

## Update

Assign the current item to yourself:

```bash
pnpm --filter skill-issues cli assign <slug> --me
```

Set status:

```bash
pnpm --filter skill-issues cli status <slug> open
pnpm --filter skill-issues cli status <slug> in_progress
pnpm --filter skill-issues cli status <slug> done
```

## Export

Export generated Markdown only when explicitly needed for compatibility or review:

```bash
pnpm --filter skill-issues cli export todo
```

Treat exported Markdown as derived output, not as an editable source of truth.
