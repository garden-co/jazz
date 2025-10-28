import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "codemod-upgrade-0-18",
    testTimeout: 10_000,
  },
});
