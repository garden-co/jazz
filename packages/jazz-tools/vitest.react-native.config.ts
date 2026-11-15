import { defineConfig } from "vitest/config";
import { resolve } from "node:path";
export default defineConfig({
  resolve: {
    alias: [
      { find: /^jazz-tools$/, replacement: resolve(__dirname, "src/index.ts") },
      { find: /^react-native$/, replacement: resolve(__dirname, "tests/react-native/ui-host.ts") },
      {
        find: "jazz-rn/relay",
        replacement: resolve(__dirname, "../../crates/jazz-rn/src/relay.ts"),
      },
      {
        find: "./NativeJazzRelay",
        replacement: resolve(__dirname, "tests/react-native/native-platform.ts"),
      },
    ],
  },
  test: {
    environment: "node",
    pool: "forks",
    include: ["tests/react-native/**/*.test.{ts,tsx}"],
    testTimeout: 15000,
  },
});
