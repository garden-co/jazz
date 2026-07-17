import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    fileParallelism: false,
    include: ["*.test.ts", "server/**/*.test.ts", "src/**/*.test.ts"],
  },
});
