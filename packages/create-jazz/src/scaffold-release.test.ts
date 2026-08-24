import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { scaffold } from "./scaffold.js";

const { tigedMock } = vi.hoisted(() => ({ tigedMock: vi.fn() }));

vi.mock("tiged", () => ({ default: tigedMock }));

const packageVersion = (
  JSON.parse(fs.readFileSync(path.resolve(import.meta.dirname, "../package.json"), "utf8")) as {
    version: string;
  }
).version;

describe("scaffold() release source snapshots", () => {
  let tmpRoot: string;
  let targetDir: string;
  let previousStarterPath: string | undefined;

  beforeEach(() => {
    previousStarterPath = process.env.JAZZ_STARTER_PATH;
    delete process.env.JAZZ_STARTER_PATH;
    tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-release-"));
    targetDir = path.join(tmpRoot, "app");
    tigedMock.mockImplementation(() => ({
      clone: async (dir: string) => {
        await fs.promises.writeFile(
          path.join(dir, "package.json"),
          JSON.stringify({ name: "release-starter" }),
        );
      },
    }));
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("packages: []\n")),
    );
  });

  afterEach(() => {
    if (previousStarterPath === undefined) delete process.env.JAZZ_STARTER_PATH;
    else process.env.JAZZ_STARTER_PATH = previousStarterPath;
    vi.unstubAllGlobals();
    tigedMock.mockReset();
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  });

  it("uses the installed release for both starter and workspace metadata", async () => {
    await scaffold({
      appName: "release-app",
      targetDir,
      pm: null,
      starter: "next-betterauth",
      git: false,
    });

    const releaseRef = `v${packageVersion}`;
    expect(tigedMock).toHaveBeenCalledWith(
      `garden-co/jazz/starters/next-betterauth#${releaseRef}`,
      { disableCache: true },
    );
    expect(vi.mocked(fetch)).toHaveBeenCalledWith(
      `https://raw.githubusercontent.com/garden-co/jazz/refs/tags/${releaseRef}/pnpm-workspace.yaml`,
      expect.anything(),
    );
  });

  it("fails clearly instead of falling back to main when the release tag is unavailable", async () => {
    tigedMock.mockImplementation(() => ({
      clone: async () => {
        throw new Error("could not find commit hash");
      },
    }));

    await expect(
      scaffold({
        appName: "unavailable-release",
        targetDir,
        pm: null,
        starter: "next-betterauth",
        git: false,
      }),
    ).rejects.toThrow(/immutable source snapshot.*does not fall back to main/i);
    expect(fs.existsSync(targetDir)).toBe(false);
  });

  it("fails clearly when the release tag lacks workspace metadata", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("not found", { status: 404 })),
    );

    await expect(
      scaffold({
        appName: "missing-workspace",
        targetDir,
        pm: null,
        starter: "next-betterauth",
        git: false,
      }),
    ).rejects.toThrow(/immutable source snapshot.*does not fall back to main/i);
    expect(fs.existsSync(targetDir)).toBe(false);
  });
});
