import { describe, expect, it } from "vitest";
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
});
