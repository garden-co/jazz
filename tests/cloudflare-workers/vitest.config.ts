import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "cloudflare-workers",
    testTimeout: 10_000,
  },
});
