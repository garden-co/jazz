---
name: changeset
description: Generate changeset files for versioning and changelog management in this monorepo.
---

# Generating Changesets

Use this skill when the user asks to create a changeset or add a changeset.

## What is a Changeset?

A changeset is a markdown file with YAML front matter that documents:
- Which packages need to be released
- What semver bump type (major, minor, or patch) each package should receive
- A changelog entry describing the changes

## Changeset File Format

Changeset files are stored in `.changeset/` directory with a unique filename: `{unique-id}.md`

```markdown
---
"package-name": major|minor|patch
"another-package": minor
---

A description of the changes that will appear in the changelog.
```

## Package Names

Use the exact package name from the package's `package.json` file. Common packages:

- `cojson`
- `jazz-tools`
- `jazz-run`
- `jazz-webhook`
- `cojson-core-wasm`
- `cojson-core-rn`
- `cojson-core-napi`
- `cojson-storage-indexeddb`
- `cojson-storage-sqlite`
- `cojson-storage-do-sqlite`
- `cojson-transport-ws`
- `community-jazz-vue`
- `create-jazz-app`
- `hash-slash`
- `quint-ui`

**Important**: Always check the actual `package.json` file to get the exact package name.

## Semver Bump Types

- **major**: Never use this, we are still in v0
- **minor**: Breaking changes that require users to update their code
- **patch**: New features that are backward compatible, bug fixes and small changes

**Important**: `minor` is only for breaking changes. Everything else should be `patch`.

## Example Changeset

```markdown
---
"jazz-tools": patch
---

Added new `useSuspenseCoState` hook for data fetching using Suspense
```

## Best Practices

- Write clear, user-facing changelog entries (not technical commit messages)
- Use past tense ("Added feature" not "Add feature")
- Be specific about what changed
- If multiple packages are affected, list them all in the front matter

## Workflow

1. User describes the change they made
2. Identify which packages are affected
3. Determine the appropriate semver bump type
4. Create the changeset file in `.changeset/` with a unique filename
