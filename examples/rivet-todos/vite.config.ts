import { defineConfig, loadEnv } from "vite";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig(({ mode }) => ({
  // loadEnv is server-only; none of these secrets has a VITE_ prefix.
  plugins: [jazzBackend(loadEnv(mode, process.cwd(), ""))],
  optimizeDeps: { exclude: ["jazz-wasm"] },
}));
