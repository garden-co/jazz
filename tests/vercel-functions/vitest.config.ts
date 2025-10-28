import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "vercel-functions",
    environment: "node",
    include: ["tests/*.test.ts"],
    testTimeout: 30000,
  },
});
