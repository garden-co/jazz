import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import { jazzPlugin } from "jazz-tools/dev/vite";

const root = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  plugins: [jazzPlugin()],
  build: {
    rollupOptions: {
      input: {
        main: resolve(root, "index.html"),
        diagnostic: resolve(root, "diagnostic.html"),
      },
    },
  },
});
