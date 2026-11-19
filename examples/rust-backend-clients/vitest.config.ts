import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
    // Each file builds its Rust binaries (cargo) in beforeAll, which takes
    // minutes cold.
    hookTimeout: 900_000,
    testTimeout: 180_000,
  },
});
