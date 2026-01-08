import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "cojson",
    include: ["src/**/*.test.ts"],
  },
});
