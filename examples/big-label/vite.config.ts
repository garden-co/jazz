import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { defineConfig } from "vite";

// Fixture/scenario tests are framework-neutral and need no broker server. This
// avoids a Vite-owned server handle (and stale local schema catalogue) in Vitest.
export default defineConfig({
  plugins: process.env.VITEST ? [react()] : [react(), jazzPlugin()],
});
