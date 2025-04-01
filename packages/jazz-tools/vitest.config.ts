import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    typecheck: {
      enabled: true,
      include: ["**/*.dtest.ts"],
      checker: "tsc",
    },
  },
});
