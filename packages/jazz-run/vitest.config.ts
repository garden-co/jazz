import { defineProject } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

export default defineProject({
  test: {
    name: "jazz-run",
    include: ["src/**/*.test.ts"],
  },
});
