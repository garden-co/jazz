import codspeedPlugin from "@codspeed/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [codspeedPlugin()],
  test: {
    benchmark: {
      include: ["benchmarks/*.bench.mjs"],
      outputJson: "benchmarks/.generated/results.json",
    },
  },
});
