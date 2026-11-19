jest.doMock("../NativeJazzRelay", () => ({ __esModule: true, default: null }));

const { decodeNativeForegroundResponse, encodeNativeForegroundCommand } =
  require("../relay") as typeof import("../relay");

test("encrypted foreground commands and replies use the Rust ABI 7 bytes", () => {
  const bytes = (...values: number[]) => Uint8Array.of(...values);
  expect(encodeNativeForegroundCommand({ type: "encryptionOpen" })).toEqual(bytes(41));
  expect(encodeNativeForegroundCommand({ type: "takeStoreRequests" })).toEqual(bytes(42));
  expect(
    encodeNativeForegroundCommand({ type: "replyStore", id: "2", ok: true, payload: "null" }),
  ).toEqual(bytes(43, 1, 50, 1, 4, 110, 117, 108, 108));
  expect(
    encodeNativeForegroundCommand({ type: "encryptionCall", op: "state", argsJson: "{}" }),
  ).toEqual(bytes(44, 5, 115, 116, 97, 116, 101, 2, 123, 125));
  expect(encodeNativeForegroundCommand({ type: "takeEncryptionReports" })).toEqual(bytes(45));

  expect(decodeNativeForegroundResponse(bytes(32, 0))).toEqual({
    type: "encryptionOpen",
    ready: false,
  });
  expect(decodeNativeForegroundResponse(bytes(33, 2, 3, 111, 110, 101, 3, 116, 119, 111))).toEqual({
    type: "storeRequests",
    requests: ["one", "two"],
  });
  expect(decodeNativeForegroundResponse(bytes(34, 1))).toEqual({
    type: "storeReply",
    accepted: true,
  });
  expect(decodeNativeForegroundResponse(bytes(35, 4, 110, 117, 108, 108))).toEqual({
    type: "encryptionResult",
    json: "null",
  });
  expect(decodeNativeForegroundResponse(bytes(36, 0))).toEqual({
    type: "encryptionReports",
    reports: [],
  });
  expect(() => decodeNativeForegroundResponse(bytes(33, 2, 0))).toThrow();
  expect(() => decodeNativeForegroundResponse(bytes(34, 2))).toThrow();
});
