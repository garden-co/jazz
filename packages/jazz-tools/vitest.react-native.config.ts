import { defineConfig } from "vitest/config";
import { resolve } from "node:path";
export default defineConfig({
  resolve: {
    alias: [
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
