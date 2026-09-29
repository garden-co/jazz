import { defineConfig, mergeConfig } from "vite";
import base from "./vite.config.ts";
export default mergeConfig(
  base,
  defineConfig({
    server: {
      port: 5287,
      strictPort: true,
      fs: { allow: ["/home/user/jazz", "/home/user/epic-drop-wt"] },
    },
  }),
);
