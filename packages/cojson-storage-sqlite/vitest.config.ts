import { defineProject } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

export default defineProject({
  test: {
    name: "cojson-storage-sqlite",
    include: ["src/**/*.test.ts"],
  },
});
