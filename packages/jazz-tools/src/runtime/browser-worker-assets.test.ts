import { readFileSync } from "node:fs";
import ts from "typescript";
import { afterEach, describe, expect, it, vi } from "vitest";
import { METRO_WORKER_API_ERROR } from "./metro-setup-error.js";
import * as standard from "./browser-worker-assets.js";
import * as metro from "./browser-worker-assets.metro.js";
import * as native from "./browser-worker-assets.native.js";

describe("browser worker asset modules", () => {
  it("export the same names for every bundler", () => {
    // Bundlers swap these modules by path, so importers see whichever one
    // their bundler chose under the same names.
    const names = Object.keys(standard).sort();
    expect(Object.keys(metro).sort()).toEqual(names);
    expect(Object.keys(native).sort()).toEqual(names);
  });

  it("leave the worker and the binary to bundlers that follow URL literals", () => {
    expect(standard.bundledWasmUrl()).toBeNull();
    expect(standard.bundledBrowserWorkerType()).toBeNull();
    expect(metro.bundledBrowserWorkerType()).toBe("classic");
  });

  // G62-B2: Expo resolves `require.unstable_resolveWorker("…")` at build
  // time (@expo/metro-config's collect-dependencies rewrites exactly that
  // call, its argument a string literal), so the property never exists at
  // runtime. Any other read of it (a `typeof` guard) survives the rewrite and
  // sees nothing, so Expo web always failed with METRO_WORKER_API_ERROR.
  it("names Expo's worker only in the call Expo rewrites", () => {
    const source = readFileSync(
      new URL("./browser-worker-assets.metro.ts", import.meta.url),
      "utf8",
    );
    const file = ts.createSourceFile("metro.ts", source, ts.ScriptTarget.Latest, true);
    const reads: string[] = [];
    const visit = (node: ts.Node) => {
      if (
        ts.isPropertyAccessExpression(node) &&
        node.name.text === "unstable_resolveWorker" &&
        ts.isIdentifier(node.expression) &&
        node.expression.text === "require"
      ) {
        const call = node.parent;
        const rewritten =
          ts.isCallExpression(call) &&
          call.expression === node &&
          call.arguments.length === 1 &&
          ts.isStringLiteral(call.arguments[0]!);
        if (!rewritten) reads.push(node.parent.getText(file));
      }
      ts.forEachChild(node, visit);
    };
    visit(file);
    expect(reads).toEqual([]);
  });

  // Expo doesn't document require.unstable_resolveWorker: a Metro without it
  // gets an error that names it, not a TypeError while the config builds.
  describe("on a Metro without require.unstable_resolveWorker", () => {
    afterEach(() => vi.unstubAllGlobals());

    it("fails by name", () => {
      vi.stubGlobal(
        "require",
        Object.assign(() => "", {}),
      );
      expect(() => metro.bundledBrowserWorkerUrl()).toThrow(METRO_WORKER_API_ERROR);
    });

    it("fails by name with no module require at all", () => {
      expect(() => metro.bundledBrowserWorkerUrl()).toThrow(METRO_WORKER_API_ERROR);
    });
  });
});
