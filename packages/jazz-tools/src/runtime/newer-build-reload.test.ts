import { afterEach, describe, expect, it, vi } from "vitest";
import {
  browserBuildId,
  newerBuildReloadAllowed,
  reloadForNewerBuild,
  setNewerBuildHandler,
} from "./newer-build-reload.js";
import type { WasmSchema } from "../drivers/types.js";

function sessionStorageStub() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => void values.set(key, value),
  };
}

const v1 = {
  todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
} as unknown as WasmSchema;
const v2 = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: true },
    ],
  },
} as unknown as WasmSchema;

afterEach(() => setNewerBuildHandler(undefined));

describe("reloading for a newer build", () => {
  it("names a build by its exact schema", () => {
    expect(browserBuildId(v1)).toBe(browserBuildId(structuredClone(v1)));
    expect(browserBuildId(v1)).not.toBe(browserBuildId(v2));
  });

  it("reloads at once without a handler, and never again from the same build in this session", () => {
    const storage = sessionStorageStub();
    const reload = vi.fn();
    const build = browserBuildId(v1);
    expect(newerBuildReloadAllowed(build, { storage })).toBe(true);
    reloadForNewerBuild(build, { storage, reload });
    expect(reload).toHaveBeenCalledOnce();
    // It loaded the same build again (a stale CDN, a rollback): however long
    // it keeps being refused as older, it doesn't reload again.
    expect(newerBuildReloadAllowed(build, { storage })).toBe(false);
    // Once on another build, a later newer deploy reloads it as usual.
    expect(newerBuildReloadAllowed(browserBuildId(v2), { storage })).toBe(true);
  });

  it("hands the reload to the app's onNewerBuild, which calls it when it suits it", () => {
    const storage = sessionStorageStub();
    const reload = vi.fn();
    let later!: () => void;
    setNewerBuildHandler((go) => (later = go));
    reloadForNewerBuild("build", { storage, reload });
    expect(reload).not.toHaveBeenCalled();
    later();
    expect(reload).toHaveBeenCalledOnce();
  });

  it("reloads anyway when the app's handler throws", () => {
    const reload = vi.fn();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    setNewerBuildHandler(() => {
      throw new Error("app bug");
    });
    reloadForNewerBuild("build", { storage: sessionStorageStub(), reload });
    expect(reload).toHaveBeenCalledOnce();
    warn.mockRestore();
  });
});
