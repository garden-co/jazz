import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [solidPlugin(), jazzPlugin()],
});
