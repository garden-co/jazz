import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, Vite proxies the app's server (anonymous sign-in, sealing and writes).
export default defineConfig({
  plugins: [react()],
  build: { target: "es2022" },
  server: { proxy: { "/api": "http://127.0.0.1:4320" } },
});
