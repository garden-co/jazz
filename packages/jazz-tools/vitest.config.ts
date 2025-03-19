import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    typecheck: {
      enabled: true,
      include: [
        "**/*.types.test.ts",
        "src/schema/coMapWithZod.test.ts",
        "src/schema/coMapWithZod.load.test.ts",
      ],
      checker: "tsc",
    },
  },
});
