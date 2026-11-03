/**
 * The hero example variants the examples E2E gate installs, builds and drives
 * as an adopter would. Together they are the tested environment matrix that
 * replaced the starters (see dev/EXAMPLES_AND_BENCHMARKS_PROGRAM.md).
 *
 * Each variant's directory must work as a standalone project once its
 * `workspace:` and `catalog:` ranges are resolved: no paths outside the
 * directory in `build` or `start`, a `test:e2e` script, and a Playwright
 * config that starts the production server (`pnpm start`) when
 * JAZZ_E2E_PROD=1.
 *
 * `publicPrefix` is the framework's browser-visible env prefix (default
 * NEXT_PUBLIC_); the harness sets `<prefix>JAZZ_APP_ID` and APP_ORIGIN.
 *
 * `env` is added to the harness environment for build, start and tests. Use it
 * for settings a production build requires, or to match values a variant's
 * build script pins.
 */
export const VARIANTS = Object.freeze([
  Object.freeze({
    name: "band-chat",
    dir: "examples/band-chat/apps/nextjs-betterauth",
    environment: "Next.js, Jazz in the app, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "jamazon",
    dir: "examples/jamazon/apps/nextjs-betterauth",
    environment: "Next.js hybrid: guest cart, then Better Auth",
    env: Object.freeze({ PAYMENT_PROVIDER: "sandbox" }),
  }),
  Object.freeze({
    name: "stage-plan-sveltekit",
    dir: "examples/stage-plan/apps/sveltekit-hybrid",
    environment: "SvelteKit hybrid: local-first guest, then Better Auth",
    publicPrefix: "PUBLIC_",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "jamazon-warehouse",
    dir: "examples/jamazon-warehouse",
    environment: "Next.js, server-seeded warehouse operations, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "wequencer",
    dir: "examples/wequencer/apps/next-betterauth",
    environment: "Next.js, realtime collaboration, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "band-book",
    dir: "examples/band-book/apps/nextjs-betterauth",
    environment: "Next.js, nested pages and invite links, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "poster-shop",
    dir: "examples/poster-shop/apps/nextjs-betterauth",
    environment: "Next.js, live canvas and image uploads, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "record-player",
    dir: "examples/record-player/apps/next-betterauth",
    environment: "Next.js, large media values, Better Auth",
    env: Object.freeze({}),
  }),
  Object.freeze({
    name: "music-agent",
    dir: "examples/music-agent/apps/nextjs",
    environment: "Next.js, backend agent writing as the server",
    // `pnpm build` pins these, so the server must start with the same values.
    env: Object.freeze({
      NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000",
      NEXT_PUBLIC_JAZZ_APP_ID: "00000000-0000-0000-0000-00000000a6e7",
      BETTER_AUTH_SECRET: "music-agent-build-better-auth-secret-0123456789",
    }),
  }),
  Object.freeze({
    name: "big-label",
    dir: "examples/big-label",
    environment: "Next.js, organizations and members, Better Auth",
    // `pnpm build` pins these, so the server must start with the same values.
    env: Object.freeze({
      NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000",
      NEXT_PUBLIC_JAZZ_APP_ID: "00000000-0000-0000-0000-000000000301",
      BETTER_AUTH_SECRET: "2ff8ef66a6ee1a1e551a8f0c7dfa9c8d6fd4cc683a963f04989fdd84cc9cdb53",
    }),
  }),
]);

export function findVariant(name) {
  return VARIANTS.find((variant) => variant.name === name);
}
