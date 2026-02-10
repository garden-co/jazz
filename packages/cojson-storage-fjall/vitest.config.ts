import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "cojson-storage-fjall",
    include: ["src/**/*.test.ts"],
  },
});
