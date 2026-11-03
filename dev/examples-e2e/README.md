# Examples E2E

The release gate that replaced the starters. Each hero example variant in
`variants.mjs` is installed the way an adopter would install it, outside the
workspace and from the packed `jazz-tools`, `jazz-napi` and `jazz-wasm`
tarballs. The harness then builds it for production, starts it and drives it
with its Playwright suite.

```sh
pnpm build:examples-e2e                      # release WASM, NAPI and packages
node dev/examples-e2e/run.mjs band-chat      # one variant
node dev/examples-e2e/run.mjs --all --keep   # every variant, keep the staged copies
```

`.github/workflows/examples-e2e.yml` runs every variant on the Version Packages
head, against the release preview's packages, and the publish gate
(`dev/scripts/release-test-gate.mjs`) requires it to pass.

## Adding a variant

A variant directory (`examples/<app>/apps/<framework>-<auth>`) must:

- work as a standalone project once its `workspace:` and `catalog:` ranges are
  resolved, so `build` and `start` never reach outside the directory;
- depend on `@playwright/test` and have a `test:e2e` script;
- have a `playwright.config.ts` that starts `pnpm start` when `JAZZ_E2E_PROD=1`,
  on the origin in `APP_ORIGIN`;
- keep its data in `JAZZ_DATA_PATH`, read its app id from
  `<prefix>JAZZ_APP_ID` and its Better Auth secret from `BETTER_AUTH_SECRET`.
  The prefix is the framework's public env prefix (`NEXT_PUBLIC_` by default;
  set `publicPrefix`, e.g. `PUBLIC_` for SvelteKit). The harness sets fresh
  values for each run.

Then add it to `variants.mjs` and to the matrix in `examples-e2e.yml`;
`dev/gates/test/examples-e2e.test.mjs` checks both lists and the contract above.
