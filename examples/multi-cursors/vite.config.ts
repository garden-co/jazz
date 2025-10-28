import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    minify: "esbuild",
    terserOptions: {
      compress: {
        drop_console: true,
      },
    },
  },
});
