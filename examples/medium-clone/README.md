# Medium Clone — Jazz Branches Demo

A small CMS-style example that shows off Jazz's query-scoped branches:

- **Front page** lists every published article.
- **New article** creates a blank article (unpublished) and a branch scoped to
  that single article. Every edit happens on the branch — main readers see
  nothing until you publish.
- **Editing a published article** opens (or reuses) a per-user draft branch
  scoped to that article. The preview pane shows your branch-local edits;
  other readers still see the published main version.
- **Drafts** live in a `drafts` table whose row ids double as branch ids.
  Row-level policies make a draft visible only to its owner, which is what
  enforces "drafts only visible to the user that created them" — the branch
  scope alone doesn't gate access; the backing row's read policy does.
- **Publish** is `branch.merge()` followed by deleting the draft metadata.

The interesting Jazz bits:

- `schema.ts` — `articles` and `drafts` tables.
- `permissions.ts` — `policy.articles.forBranch(policy.drafts, ...)` declares
  that a `drafts` row can act as a branch anchor for `articles`, plus the
  branch-scoped read/write rules.
- `src/branchScope.ts` — tiny helper that overlays `branch` + `branchScope`
  fields onto a query JSON so `useAll` can read from a branch reactively.
- `src/FrontPage.tsx` — creates the article + draft row + branch scope in
  `startNew`, and reuses an existing draft when editing a published article.
- `src/ArticleEditor.tsx` — writes through `{ branch: draftId }` and calls
  `db.branch(draftId).merge()` on publish.

## Run

```
pnpm install
pnpm dev
```

The `.env` file already has a `VITE_JAZZ_APP_ID`. Set `VITE_JAZZ_SERVER_URL` if
you want to sync with a server; otherwise everything stays in local-first
storage.
