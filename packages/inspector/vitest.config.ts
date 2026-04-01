import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react-swc";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      {
        find: "jazz-tools/react",
        replacement: resolve(__dirname, "../jazz-tools/src/react/index.ts"),
      },
      {
        find: "jazz-tools/testing",
        replacement: resolve(__dirname, "../jazz-tools/src/testing/index.ts"),
      },
      {
        find: "jazz-tools",
        replacement: resolve(__dirname, "../jazz-tools/src/index.ts"),
      },
    ],
  },
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["./src/test/setup.ts"],
  },
});
