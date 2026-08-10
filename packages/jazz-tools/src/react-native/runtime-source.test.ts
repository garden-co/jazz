import { describe, expect, it, vi } from "vitest";
import type { JazzRnModule } from "./native-db.js";
import {
  ReactNativeRuntimeSource,
  resolveReactNativePersistentPath,
  sanitizeReactNativeDbName,
} from "./runtime-source.js";

const { importJazzRn } = vi.hoisted(() => ({
  importJazzRn: vi.fn(),
}));

vi.mock("./jazz-rn-importer.js", () => ({ importJazzRn }));

describe("ReactNativeRuntimeSource", () => {
  it("builds a safe SQLite path from the persistent namespace", () => {
    expect(
      resolveReactNativePersistentPath({
        appId: "chat/app",
        dataDirectory: "/var/mobile/Documents/",
      }),
    ).toBe("/var/mobile/Documents/chat_app.db");
    expect(sanitizeReactNativeDbName("a b/c.d:_-Z9")).toBe("a_b_c_d__-Z9");
  });

  it("requires an absolute data directory only for persistent opens", () => {
    expect(() => resolveReactNativePersistentPath({ appId: "chat" })).toThrow(
      "requires an absolute dataDirectory",
    );
    expect(() =>
      resolveReactNativePersistentPath({ appId: "chat", dataDirectory: "relative" }),
    ).toThrow("must be an absolute filesystem path");
  });

  it("passes caller-supplied token time through to the native module", () => {
    const mintLocalFirstToken = vi.fn(() => "local-token");
    const mintAnonymousToken = vi.fn(() => "anonymous-token");
    const source = new ReactNativeRuntimeSource();
    Object.assign(source, {
      module: {
        mintLocalFirstToken,
        mintAnonymousToken,
      } as unknown as JazzRnModule,
    });

    expect(
      source.mintLocalFirstToken({
        secret: "secret",
        audience: "app",
        ttlSeconds: 3600,
        nowSeconds: 1_234_567n,
      }),
    ).toBe("local-token");
    expect(
      source.mintAnonymousToken({
        secret: "secret",
        audience: "app",
        ttlSeconds: 60,
        nowSeconds: 9_876n,
      }),
    ).toBe("anonymous-token");
    expect(mintLocalFirstToken).toHaveBeenCalledWith("secret", "app", 3600, 1_234_567n);
    expect(mintAnonymousToken).toHaveBeenCalledWith("secret", "app", 60, 9_876n);
  });

  it("explains how to install a missing jazz-rn peer", async () => {
    importJazzRn.mockRejectedValueOnce(
      Object.assign(new Error("Cannot find package 'jazz-rn' imported from runtime-source"), {
        code: "ERR_MODULE_NOT_FOUND",
      }),
    );

    await expect(new ReactNativeRuntimeSource().load({ appId: "chat" })).rejects.toThrow(
      'The "jazz-rn" peer dependency is required',
    );
  });

  it("does not misdiagnose a missing transitive dependency", async () => {
    const transitiveError = Object.assign(
      new Error("Cannot find package 'native-helper' imported from jazz-rn"),
      { code: "ERR_MODULE_NOT_FOUND" },
    );
    importJazzRn.mockRejectedValueOnce(transitiveError);

    await expect(new ReactNativeRuntimeSource().load({ appId: "chat" })).rejects.toBe(
      transitiveError,
    );
  });
});
