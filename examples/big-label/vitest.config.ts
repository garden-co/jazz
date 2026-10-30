import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The configuration the API routes read, as `pnpm build` supplies it.
    // Nothing listens on these addresses during unit tests.
    env: {
      NEXT_PUBLIC_APP_ORIGIN: "http://127.0.0.1:3000",
      NEXT_PUBLIC_JAZZ_APP_ID: "00000000-0000-0000-0000-000000000301",
      NEXT_PUBLIC_JAZZ_SERVER_URL: "http://127.0.0.1:4200",
    },
  },
});
