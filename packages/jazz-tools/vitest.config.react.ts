import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

/**
 * Vitest configuration for React component/hook tests in react-core.
 * Uses happy-dom and @vitejs/plugin-react.
 * This configuration uses happy-dom because it exercises React hooks.
 */
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.tsx"],
    exclude: ["node_modules/**", "src/solid/**"],
  },
});
