import { defineProject } from "vitest/config";

export default defineProject({
  plugins: [],
  test: {
    name: "cojson-core-wasm",
    include: ["__test__/**/*.test.{js,ts,tsx}"],
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
  },
});
