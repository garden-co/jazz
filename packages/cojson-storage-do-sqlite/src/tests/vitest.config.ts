import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "cloudflare-do-storage",
    testTimeout: 10_000,
    include: ["*.test.ts"],
  },
});
