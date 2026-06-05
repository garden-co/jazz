import { defineConfig } from "vitest/config";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  test: {
    environment: "jsdom",
    include: ["src/solid/**/*.test.ts", "src/solid/**/*.test.tsx"],
    exclude: ["node_modules/**"],
  },
});
