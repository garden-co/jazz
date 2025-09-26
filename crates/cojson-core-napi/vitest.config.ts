import { defineProject } from "vitest/config";

export default defineProject({
  plugins: [],
  test: {
    name: "cojson-core-napi",
    include: ["__test__/**/*.test.{js,ts,tsx}"],
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
  },
});
