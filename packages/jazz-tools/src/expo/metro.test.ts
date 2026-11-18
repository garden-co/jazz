import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { withJazz } from "./metro.js";

// An installed jazz-tools as Metro resolves it: the browser asset module with
// its Metro counterpart beside it.
const install = mkdtempSync(join(tmpdir(), "jazz-metro-"));
const runtimeDir = join(install, "node_modules/jazz-tools/dist/runtime");
mkdirSync(runtimeDir, { recursive: true });
const standardAssets = join(runtimeDir, "browser-worker-assets.js");
const metroAssets = join(runtimeDir, "browser-worker-assets.metro.js");
writeFileSync(standardAssets, "");
writeFileSync(metroAssets, "");
afterAll(() => rmSync(install, { recursive: true, force: true }));

/** Metro's default resolver, standing in as `context.resolveRequest`. */
function metroContext(filePath: string) {
  return { resolveRequest: vi.fn(() => ({ type: "sourceFile", filePath })) };
}

function resolveWith(
  config: Parameters<typeof withJazz>[0],
  filePath: string,
  platform: string | null,
) {
  const { resolveRequest } = withJazz(config).resolver as {
    resolveRequest: (context: unknown, moduleName: string, platform: string | null) => unknown;
  };
  return resolveRequest(metroContext(filePath), "./browser-worker-assets.js", platform);
}

describe("withJazz", () => {
  it("is published as jazz-tools/expo/metro", () => {
    const packageJson = JSON.parse(
      readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
    ) as { exports: Record<string, unknown> };
    expect(packageJson.exports["./expo/metro"]).toEqual({
      types: "./dist/expo/metro.d.ts",
      default: "./dist/expo/metro.js",
    });
  });

  it("lets Metro serve and export the WASM binary as an asset", () => {
    const config = withJazz({ resolver: { assetExts: ["png", "ttf"] } });
    expect(config.resolver.assetExts).toEqual(["png", "ttf", "wasm"]);
  });

  it("keeps an asset list that already has wasm, and the rest of the config", () => {
    const assetExts = ["png", "wasm"];
    const config = withJazz({
      projectRoot: "/app",
      resolver: { assetExts, sourceExts: ["ts"] },
    });
    expect(config.resolver.assetExts).toBe(assetExts);
    expect(config).toMatchObject({ projectRoot: "/app", resolver: { sourceExts: ["ts"] } });
  });

  it("gives the web platform the asset module that takes the worker and WASM from Metro", () => {
    expect(resolveWith({ resolver: {} }, standardAssets, "web")).toEqual({
      type: "sourceFile",
      filePath: metroAssets,
    });
  });

  it.each(["ios", "android", null])("leaves the %s platform's resolution alone", (platform) => {
    expect(resolveWith({ resolver: {} }, standardAssets, platform)).toEqual({
      type: "sourceFile",
      filePath: standardAssets,
    });
  });

  it("leaves other modules alone on web", () => {
    const other = join(runtimeDir, "runtime-config.js");
    expect(resolveWith({ resolver: {} }, other, "web")).toEqual({
      type: "sourceFile",
      filePath: other,
    });
    // A module of the same name without a Metro counterpart is not Jazz's.
    const foreign = join(install, "node_modules/other/runtime/browser-worker-assets.js");
    expect(resolveWith({ resolver: {} }, foreign, "web")).toEqual({
      type: "sourceFile",
      filePath: foreign,
    });
  });

  it("runs a resolver the config already has, then applies Jazz's on its result", () => {
    const upstream = vi.fn((_context: unknown, _moduleName: string, _platform: string | null) => ({
      type: "sourceFile",
      filePath: standardAssets,
    }));
    const context = metroContext("/unused");
    const { resolveRequest } = withJazz({ resolver: { resolveRequest: upstream } }).resolver;

    expect(resolveRequest!(context, "jazz-tools", "web")).toEqual({
      type: "sourceFile",
      filePath: metroAssets,
    });
    expect(upstream).toHaveBeenCalledWith(context, "jazz-tools", "web");
    expect(context.resolveRequest).not.toHaveBeenCalled();
  });

  it("can wrap a config twice", () => {
    const config = withJazz(withJazz({ resolver: { assetExts: ["png"] } }));
    expect(config.resolver.assetExts).toEqual(["png", "wasm"]);
    expect(resolveWith(config, standardAssets, "web")).toEqual({
      type: "sourceFile",
      filePath: metroAssets,
    });
  });
});
