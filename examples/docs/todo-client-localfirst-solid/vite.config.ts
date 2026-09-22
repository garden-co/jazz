import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "jazz-tools/solid": fileURLToPath(
        new URL("../../../packages/jazz-tools/src/solid/index.ts", import.meta.url),
      ),
    },
  },
  plugins: [solidPlugin(), jazzPlugin()],
});
