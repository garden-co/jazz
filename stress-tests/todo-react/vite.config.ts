import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [
    react(),
    jazzPlugin({
      appId: "9630ff6b-b95b-4675-be7a-e083975ff412",
    }),
  ],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
});
