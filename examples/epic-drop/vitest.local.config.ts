import { defineConfig, mergeConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import base from "./vitest.config.browser.ts";

export default mergeConfig(
  base,
  defineConfig({
    server: { fs: { allow: ["/home/user/jazz", "/home/user/epic-drop-wt"] } },
    test: {
      browser: {
        provider: playwright({
          launchOptions: { executablePath: "/opt/pw-browsers/chromium-1194/chrome-linux/chrome" },
        }),
      },
    },
  }),
);
