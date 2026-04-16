import { defineConfig } from "vite";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  plugins: [jazzPlugin()],
});
