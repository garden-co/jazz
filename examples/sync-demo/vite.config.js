import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174, // Different port from demo-app
  },
  optimizeDeps: {
    exclude: ["groove-wasm"],
  },
});
