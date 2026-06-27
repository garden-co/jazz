import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

/**
 * Vitest configuration for React component/hook tests in react-core.
 * Uses happy-dom and @vitejs/plugin-react.
 * Cannot share the main vitest.config.ts which uses pool:forks and ssr.noExternal:true
 * (required for node:sqlite tests, incompatible with React hooks testing).
 */
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.tsx"],
    exclude: ["node_modules/**"],
  },
});
