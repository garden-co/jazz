import { defineConfig } from "vitest/config";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
  },
  plugins: process.env.VITEST ? [] : [jazzPlugin()],
  worker: {
    format: "es",
  },
});
