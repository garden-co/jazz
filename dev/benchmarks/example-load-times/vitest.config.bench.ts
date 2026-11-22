import codspeedPlugin from "@codspeed/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [codspeedPlugin()],
  test: {
    // One app at a time: every app has its own servers and browser, and
    // parallel apps would compete for the CPU being measured.
    fileParallelism: false,
    reporters: ["default", "json"],
    outputFile: { json: ".generated/results.json" },
    benchmark: {
      include: ["benches/*.bench.mjs"],
    },
  },
});
