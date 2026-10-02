import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig(({ mode }) => ({
  plugins: [react(), jazzPlugin({ server: mode === "test" ? { inMemory: true } : true })],
}));
