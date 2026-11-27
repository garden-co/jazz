// The app's Vite config plus, for thin-reload.mjs: minify on/off and a separate outDir. The app's own config is unchanged.
import base from "../../../stage-plan/jazz/vite.config.js";

const cfg = base as Record<string, any>;

export default {
  ...cfg,
  root: new URL("../../../stage-plan/jazz/", import.meta.url).pathname,
  build: {
    ...(cfg.build ?? {}),
    outDir: process.env.THIN_PROFILE_OUT ?? "dist-profile",
    emptyOutDir: true,
    minify: process.env.THIN_PROFILE_MINIFY !== "0",
    sourcemap: true,
  },
};
