import { describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";

const repoRoot = path.resolve(import.meta.dirname, "../../../");
const BETTER_AUTH_DECLARATIONS = [
  ...[
    "next-betterauth",
    "next-hybrid",
    "sveltekit-betterauth",
    "sveltekit-hybrid",
    "react-betterauth",
    "react-hybrid",
    "ts-betterauth",
    "ts-hybrid",
  ].map((starter) => ({
    path: path.join("starters", starter, "package.json"),
    section: "dependencies",
  })),
  {
    path: "examples/auth-betterauth-chat/package.json",
    section: "dependencies",
  },
  {
    path: "packages/jazz-tools/package.json",
    section: "devDependencies",
  },
  {
    path: "packages/jazz-tools/package.json",
    section: "peerDependencies",
  },
] as const;

describe("Better Auth workspace compatibility", () => {
  it("requires every Better Auth workspace declaration to use the exact tested release", () => {
    for (const declaration of BETTER_AUTH_DECLARATIONS) {
      const packageJson = JSON.parse(
        fs.readFileSync(path.join(repoRoot, declaration.path), "utf8"),
      ) as Record<string, Record<string, string>>;

      expect(
        packageJson[declaration.section]?.["better-auth"],
        `${declaration.path} (${declaration.section})`,
      ).toBe("1.6.24");
    }
  });
});
