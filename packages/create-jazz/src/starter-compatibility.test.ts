import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";

const repoRoot = path.resolve(import.meta.dirname, "../../../");
const BETTER_AUTH_STARTERS = [
  "next-betterauth",
  "next-hybrid",
  "sveltekit-betterauth",
  "sveltekit-hybrid",
  "react-betterauth",
  "react-hybrid",
  "ts-betterauth",
  "ts-hybrid",
] as const;

describe("Better Auth starter compatibility", () => {
  it("requires every Better Auth starter to use the current stable release line", () => {
    for (const starter of BETTER_AUTH_STARTERS) {
      const packageJson = JSON.parse(
        fs.readFileSync(path.join(repoRoot, "starters", starter, "package.json"), "utf8"),
      ) as { dependencies?: Record<string, string> };

      expect(packageJson.dependencies?.["better-auth"], starter).toBe("^1.6.24");
    }
  });
});
