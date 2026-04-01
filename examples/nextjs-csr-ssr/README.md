# Jazz + Next.js example

- `pnpm run sync-server`
- `pnpm run dev`

## Hot points

- To use jazz-tools/backend with native bindings, `serverExternalPackages: ["jazz-napi", "jazz-tools"],` is needed `next.config.ts`
- Due to the symlinks made by pnpm in monorepo, the `serverExternalPackages` is not enough to avoid Next.js to skip the evaluation of crates/jazz-napi. That's why the workaround in `lib/jazz-server.ts`
