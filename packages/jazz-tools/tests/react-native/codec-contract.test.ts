import { describe, expect, test } from "vitest";
import {
  decodeNativeForegroundResponse,
  encodeNativeForegroundCommand,
  type NativeForegroundCommand,
} from "jazz-rn/relay";
import { decodeCommandInRust, rustResponseCorpus } from "./native-platform.js";

// Codec tests intentionally inspect native wire semantics: row results alone
// cannot reveal field/ordinal drift between the installed host and OTA bundle.
const txId = Uint8Array.from({ length: 16 }, () => 7);
const optionsJson = '{"readTier":"local","view":{"head":"main"}}';
const cases: [string, unknown, unknown][] = [
  ["probe", "probe", "Probe"],
  [
    "prepare",
    { type: "prepareQuery", query: Uint8Array.of(1, 128, 2) },
    { PrepareQuery: { query: [1, 128, 2] } },
  ],
  [
    "exclusive transaction",
    { type: "beginTransaction", kind: "exclusive" },
    { BeginTransaction: { kind: "Exclusive" } },
  ],
  [
    "read without transaction",
    { type: "allWithOptions", query: 128, optionsJson },
    { AllWithOptions: { query: 128, options_json: optionsJson, transaction: null } },
  ],
  [
    "relation transaction",
    { type: "allRelationSnapshotWithOptions", query: 1, optionsJson, transaction: 256 },
    { AllRelationSnapshotWithOptions: { query: 1, options_json: optionsJson, transaction: 256 } },
  ],
  [
    "subscription",
    { type: "subscribeWithOptions", query: 128, optionsJson },
    { SubscribeWithOptions: { query: 128, options_json: optionsJson } },
  ],
  [
    "settlement",
    { type: "waitForTransaction", txId, tier: "core" },
    { WaitForTransaction: { tx_id: [...txId], tier: "core" } },
  ],
  [
    "restore",
    {
      type: "stageMutation",
      transaction: 256,
      mutation: "restore",
      table: "records",
      rowId: txId,
      cells: Uint8Array.of(1, 2),
      optionsJson: "{}",
    },
    {
      StageMutation: {
        transaction: 256,
        mutation: "Restore",
        table: "records",
        row_id: [...txId],
        cells: [1, 2],
        options_json: "{}",
      },
    },
  ],
  ["disconnect", { type: "disconnectNativeUpstream" }, "DisconnectNativeUpstream"],
  ["reconnect", { type: "reconnectNativeUpstream" }, "ReconnectNativeUpstream"],
  ["status", { type: "nativeConnectionStatus" }, "NativeConnectionStatus"],
  ["metadata", { type: "nativeSessionMetadata" }, "NativeSessionMetadata"],
];

describe("RN Rust/TypeScript foreground codec contract", () => {
  test.each(cases)("%s preserves Rust semantic fields", (_name, command, expected) => {
    const bytes = encodeNativeForegroundCommand(command as NativeForegroundCommand);
    expect(decodeCommandInRust(bytes)).toEqual(expected);
    expect(() => decodeCommandInRust(Uint8Array.from([...bytes, 0]))).toThrow();
  });

  test("Rust-produced responses decode through the production reader", () => {
    const responses = rustResponseCorpus();
    expect(responses.map(decodeNativeForegroundResponse)).toEqual([
      { type: "pending", operation: 256 },
      { type: "operationError", reason: "codec boundary: λ" },
      { type: "transactionSettled", txId },
      {
        type: "nativeConnectionStatus",
        configured: true,
        explicitlyOffline: false,
        connected: true,
      },
      { type: "nativeSessionMetadata", issuer: "fixture-issuer", userId: "fixture-user" },
    ]);
    for (const response of responses) {
      expect(() =>
        decodeNativeForegroundResponse(response.subarray(0, response.length - 1)),
      ).toThrow();
      expect(() => decodeNativeForegroundResponse(Uint8Array.from([...response, 0]))).toThrow();
    }
  });

  test("noncanonical Rust request encodings fail closed", () => {
    expect(() => decodeCommandInRust(Uint8Array.of(128, 0))).toThrow();
    expect(() => decodeCommandInRust(Uint8Array.of(255))).toThrow();
    expect(() => decodeCommandInRust(Uint8Array.of(18, 1, 0, 2))).toThrow();
  });
});
