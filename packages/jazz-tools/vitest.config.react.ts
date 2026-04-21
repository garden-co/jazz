import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

/**
 * Vitest configuration for React component/hook tests in react-core.
 * Uses happy-dom and @vitejs/plugin-react.
 * Cannot share the main vitest.config.ts which uses pool:forks and ssr.noExternal:true
 * (required for node:sqlite tests, incompatible with React hooks testing).
 */
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      {
        find: "expo-crypto",
        replacement: resolve(__dirname, "test-support/expo-crypto-stub.ts"),
      },
      {
        find: "expo-secure-store",
        replacement: resolve(__dirname, "test-support/expo-secure-store-stub.ts"),
      },
    ],
  },
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.tsx"],
    exclude: ["node_modules/**"],
  },
});
