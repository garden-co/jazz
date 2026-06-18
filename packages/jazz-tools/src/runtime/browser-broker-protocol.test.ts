import { describe, expect, it } from "vitest";
import {
  BROKER_CONTROL_PROTOCOL_VERSION,
  createBrowserBrokerFingerprint,
  createRuntimeSourceIdentity,
  detectBrowserBrokerMissingCapabilities,
  formatUnsupportedBrowserBrokerError,
  type BrowserBrokerFingerprintInput,
} from "./browser-broker-protocol.js";

describe("browser broker protocol", () => {
  it("formats unsupported environment errors with the missing capabilities", () => {
    expect(formatUnsupportedBrowserBrokerError(["SharedWorker"])).toBe(
      "Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks support. This environment is missing: SharedWorker.",
    );
    expect(formatUnsupportedBrowserBrokerError(["SharedWorker", "Web Locks"])).toBe(
      "Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks support. This environment is missing: SharedWorker, Web Locks.",
    );
  });

  it("detects missing coordination capabilities", () => {
    expect(
      detectBrowserBrokerMissingCapabilities({
        SharedWorker: undefined,
        MessageChannel: class {},
        navigator: { locks: { request() {} } },
      }),
    ).toEqual(["SharedWorker"]);

    expect(
      detectBrowserBrokerMissingCapabilities({
        SharedWorker: class {},
        MessageChannel: undefined,
        navigator: {},
      }),
    ).toEqual(["MessageChannel", "Web Locks"]);
  });

  it("creates a deterministic fingerprint from stable compatibility fields", () => {
    const input: BrowserBrokerFingerprintInput = {
      appId: "app",
      dbName: "db",
      env: "dev",
      userBranch: "main",
      serverUrl: "ws://example.test",
      schemaHash: "schema-a",
      authClass: "user:stable-id",
      runtimeSourceIdentity: "default",
      persistentDriverNamespace: "db",
      storageFormatVersion: "opfs-btree-v1",
    };

    expect(createBrowserBrokerFingerprint(input)).toBe(createBrowserBrokerFingerprint(input));
    expect(createBrowserBrokerFingerprint(input)).toContain(BROKER_CONTROL_PROTOCOL_VERSION);
    expect(createBrowserBrokerFingerprint(input)).toContain("schema-a");
    expect(createBrowserBrokerFingerprint(input)).not.toContain("jwt");
    expect(
      createBrowserBrokerFingerprint({
        ...input,
        schemaHash: "schema-b",
      }),
    ).not.toBe(createBrowserBrokerFingerprint(input));
  });

  it("distinguishes custom runtime source objects", () => {
    const firstSource = new Uint8Array([0, 1, 2, 3]);
    const secondSource = new Uint8Array([0, 1, 2, 4]);

    expect(createRuntimeSourceIdentity({ wasmSource: firstSource })).not.toBe(
      createRuntimeSourceIdentity({ wasmSource: secondSource }),
    );

    const firstModule = {} as WebAssembly.Module;
    const secondModule = {} as WebAssembly.Module;

    expect(createRuntimeSourceIdentity({ wasmModule: firstModule })).not.toBe(
      createRuntimeSourceIdentity({ wasmModule: secondModule }),
    );
    expect(createRuntimeSourceIdentity({ wasmModule: firstModule })).toBe(
      createRuntimeSourceIdentity({ wasmModule: firstModule }),
    );
  });
});
