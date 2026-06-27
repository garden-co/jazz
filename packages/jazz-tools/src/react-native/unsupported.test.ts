import { describe, expect, it } from "vitest";
import {
  createReactNativeDirectCoreUnsupportedError,
  REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE,
  ReactNativeCoreSource,
} from "./runtime-module.js";
import { createDb } from "./db.js";
import { createJazzClient } from "./create-jazz-client.js";
import { loadJazzRn } from "./jazz-rn-loader.js";

describe("react-native react-native direct-core support", () => {
  it("fails fast instead of routing through a legacy bridge", async () => {
    expect(createReactNativeDirectCoreUnsupportedError().message).toBe(
      REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE,
    );

    await expect(createDb({ appId: "rn-disabled" })).rejects.toThrow(
      REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE,
    );
    await expect(createJazzClient({ appId: "rn-disabled" })).rejects.toThrow(
      REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE,
    );
    await expect(loadJazzRn()).rejects.toThrow(REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE);
  });

  it("does not mint tokens through the old jazz-rn native module", () => {
    const runtime = new ReactNativeCoreSource();

    expect(() =>
      runtime.mintLocalFirstToken({
        secret: "secret",
        audience: "app",
        ttlSeconds: 60,
        nowSeconds: 1n,
      }),
    ).toThrow(REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE);
    expect(() =>
      runtime.mintAnonymousToken({
        secret: "secret",
        audience: "app",
        ttlSeconds: 60,
        nowSeconds: 1n,
      }),
    ).toThrow(REACT_NATIVE_DIRECT_CORE_UNSUPPORTED_MESSAGE);
  });
});
