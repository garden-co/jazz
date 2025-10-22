import { defineConfig } from "vitest/config";
import { withScenario } from "@langwatch/scenario/integrations/vitest/config";

export default withScenario(
  defineConfig({
    test: {
      testTimeout: 5 * (60 * 1000), // 5 minutes
      globals: true,
      reporters: ["verbose"],
      environment: "node",
      include: ["**/*.{test,spec,vibe}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}"],
      exclude: ["node_modules", "**/node_modules/**"],
    },
  }),
);
