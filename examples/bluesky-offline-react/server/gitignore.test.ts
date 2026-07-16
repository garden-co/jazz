import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const exampleRoot = fileURLToPath(new URL("../", import.meta.url));

describe("sensitive local files", () => {
  it("ignores runtime credentials and databases but keeps the environment template", () => {
    const sensitivePaths = [
      ".env",
      ".env.local",
      ".env.production",
      "data/jazz.db",
      "data/oauth-sessions.json",
      "jazz.db",
      "jazz.db-wal",
      "jazz.db-shm",
      "oauth-sessions.json",
    ];
    const result = spawnSync(
      "git",
      ["check-ignore", "--no-index", "--stdin"],
      {
        cwd: exampleRoot,
        input: [...sensitivePaths, ".env.example"].join("\n"),
        encoding: "utf8",
      },
    );
    const ignored = result.stdout.trim().split("\n").filter(Boolean);

    expect(ignored).toEqual(sensitivePaths);
  });
});
