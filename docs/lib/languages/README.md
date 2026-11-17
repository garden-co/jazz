# Language tabs

Pages show per-language code in `<Tabs groupId="jazz-environment">`. The tab
order is TypeScript, Effect, Rust, Swift, Kotlin, Go, set by `LANGUAGES` in
`index.ts`. A test refuses tabs that are out of order, that use a language not
in the list, or whose `items` don't match the tabs.

## Adding a language

1. Write your example's snippets to [SNIPPETS.md](SNIPPETS.md): the same app,
   shape and scenarios as the TypeScript ones, in `// #region <topic>-<lang>`
   regions (`reading-oneshot-go`, `writing-crud-go`, and so on).
2. Add a `<Tab value="Go">` with an `<include>` of your snippet to each
   `jazz-environment` block whose topic you have, and add `"Go"` to that block's
   `items`. Where your language can't do a topic, leave its tab out: the
   language's "Not supported yet" list on Language Support says so.
3. Write `content/docs/setup/<lang>.mdx`, ending with a `## Not supported yet`
   list. It's already in `content/docs/meta.json`.
4. Set `bindings` in `LANGUAGES`: your committed generated file for StagePlan
   and the declarations to show for its `tasks` table (row, insert and status
   types). Then add a tab under "The generated bindings" in
   `schemas/defining-tables.mdx` that holds only `<BindingsExcerpt language="Go" />`.
   The build cuts each declaration out by name, from its first line to its
   matching close, with the doc comments and attributes right above it.
   Generated files carry no `#region` markers. A test fails if a symbol is missing.
5. Set `enabled: true`.

While a language is switched off, the build (`plugin.ts`, on each page's source
before its includes are read) drops its tabs, and `source.ts` drops its setup page and sidebar entry. So a
language that misses a release appears nowhere in that release's docs, and its
tabs can sit on main before its examples are ready.

Put language tabs and `<BindingsExcerpt>` in pages only, not in
`content/partials`: an included partial doesn't pass through the plugin, so a
test refuses them there.

## Experimental languages

`experimental: true` on a language in `LANGUAGES` makes the build put an
`<ExperimentalBadge>` at the top of each of its tabs and of its setup page,
linking to its tab on Language Support (`#jazz-environment-<language>`).
Remove the flag to drop the labels.
