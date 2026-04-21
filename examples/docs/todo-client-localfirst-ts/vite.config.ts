import { defineConfig } from "vite";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [jazzPlugin()],
});
