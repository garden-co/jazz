# Jazz + Next.js example

- `pnpm run sync-server`
- `pnpm run dev`

## Hot points

- `next.config.ts` uses `withJazz(...)` from `jazz-tools/dev/next`
- Public Jazz connection vars are `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`
- The SSR example still keeps `BACKEND_SECRET` explicit because backend access is server-only
