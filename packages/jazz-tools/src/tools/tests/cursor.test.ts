import { describe, expect, it } from "vitest";
import {
  CursorError,
  decodeAndValidateCursor,
  encodeCursor,
} from "../subscribe/cursor.js";
import type {
  CoValueCursor,
  DecodedCoValueCursor,
} from "../subscribe/types.js";
import { type SessionID } from "cojson";

const rootId = "co_zRoot";
const sessionA = "sessionA" as SessionID;
const sessionB = "sessionB" as SessionID;

const buildCursor = (
  overrides: Partial<DecodedCoValueCursor> = {},
): DecodedCoValueCursor => ({
  version: 1,
  rootId,
  resolveFingerprint: {},
  frontiers: {
    co_zRoot: {
      [sessionA]: 1,
    },
  },
  ...overrides,
});

describe("`encodeCursor` and `decodeAndValidateCursor`", () => {
  it("encodes with the cursor_z prefix", () => {
    const encoded = encodeCursor(buildCursor());

    expect(encoded.startsWith("cursor_z")).toBe(true);
  });

  it("roundtrips a valid cursor", () => {
    const decodedCursor = buildCursor({
      resolveFingerprint: { profile: { name: {} }, friends: { $each: {} } },
      frontiers: {
        co_zRoot: { [sessionA]: 3, [sessionB]: 9 },
        co_zChild: { [sessionA]: 1 },
      },
    });
    const encoded = encodeCursor(decodedCursor);

    const decoded = decodeAndValidateCursor({
      cursor: encoded,
      rootId,
      resolve: decodedCursor.resolveFingerprint,
    });

    expect(decoded).toEqual(decodedCursor);
  });

  it("produces identical cursors for equivalent resolve/frontier data regardless of key ordering", () => {
    const firstCursor = buildCursor({
      resolveFingerprint: {
        profile: { name: true, email: true },
        friends: { $each: true },
      },
      frontiers: {
        co_zRoot: { [sessionA]: 3, [sessionB]: 9 },
        co_zChild: { [sessionA]: 1, [sessionB]: 2 },
      },
    });

    const sameDataDifferentOrder = buildCursor({
      resolveFingerprint: {
        friends: { $each: true },
        profile: { email: true, name: true },
      },
      frontiers: {
        co_zChild: { [sessionB]: 2, [sessionA]: 1 },
        co_zRoot: { [sessionB]: 9, [sessionA]: 3 },
      },
    });

    expect(encodeCursor(firstCursor)).toBe(
      encodeCursor(sameDataDifferentOrder),
    );
  });

  it("normalizes cursor payloads with extra top-level properties when decoded and re-encoded", () => {
    const cursorWithExtraProperty: DecodedCoValueCursor & {
      extraMetadata: string;
    } = {
      version: 1,
      rootId,
      resolveFingerprint: {
        profile: { name: true, email: true },
        friends: { $each: true },
      },
      frontiers: {
        co_zChild: { [sessionB]: 2, [sessionA]: 1 },
        co_zRoot: { [sessionB]: 9, [sessionA]: 3 },
      },
      extraMetadata: "should-be-stripped",
    };

    const encodedWithExtraProperty = encodeCursor(cursorWithExtraProperty);

    const decoded = decodeAndValidateCursor({
      cursor: encodedWithExtraProperty,
      rootId,
      resolve: {
        friends: { $each: true },
        profile: { email: true, name: true },
      },
    });

    const canonicalExpected = buildCursor({
      resolveFingerprint: {
        profile: { name: {}, email: {} },
        friends: { $each: {} },
      },
      frontiers: {
        co_zRoot: { [sessionA]: 3, [sessionB]: 9 },
        co_zChild: { [sessionA]: 1, [sessionB]: 2 },
      },
    });

    expect(decoded).toEqual(canonicalExpected);
    expect(encodeCursor(decoded)).toBe(encodeCursor(canonicalExpected));
  });

  it("accepts resolve=true when cursor resolveFingerprint is {}", () => {
    const decodedCursor = buildCursor({ resolveFingerprint: {} });
    const encoded = encodeCursor(decodedCursor);

    const decoded = decodeAndValidateCursor({
      cursor: encoded,
      rootId,
      resolve: true,
    });

    expect(decoded).toEqual(decodedCursor);
  });

  it("accepts resolve as subset of cursor resolveFingerprint", () => {
    const decodedCursor = buildCursor({
      resolveFingerprint: { pet: {}, friends: {} },
    });
    const encoded = encodeCursor(decodedCursor);

    const decoded = decodeAndValidateCursor({
      cursor: encoded,
      rootId,
      resolve: { pet: true },
    });

    expect(decoded).toEqual(decodedCursor);
  });

  it("accepts deep `true` in resolve as subset of cursor resolveFingerprint", () => {
    const decodedCursor = buildCursor({
      resolveFingerprint: { pet: { animalFriends: {} }, friends: {} },
    });
    const encoded = encodeCursor(decodedCursor);

    const decoded = decodeAndValidateCursor({
      cursor: encoded,
      rootId,
      resolve: { pet: true },
    });

    expect(decoded).toEqual(decodedCursor);
  });

  it("throws CursorError when root CoValue ID does not match", () => {
    const encoded = encodeCursor(buildCursor());

    expect(() =>
      decodeAndValidateCursor({
        cursor: encoded,
        rootId: "co_zDifferentRoot",
        resolve: {},
      }),
    ).toThrowError(CursorError);
    expect(() =>
      decodeAndValidateCursor({
        cursor: encoded,
        rootId: "co_zDifferentRoot",
        resolve: {},
      }),
    ).toThrowError("Invalid cursor: root CoValue ID mismatch");
  });

  it("throws CursorError when resolve query does not match", () => {
    const decodedCursor = buildCursor({
      resolveFingerprint: { profile: { name: true } },
    });
    const encoded = encodeCursor(decodedCursor);

    expect(() =>
      decodeAndValidateCursor({
        cursor: encoded,
        rootId,
        resolve: { profile: { email: true } },
      }),
    ).toThrowError(
      new CursorError(
        'Invalid cursor: resolve query mismatch. Expected {"profile":{"email":{}}} to be a subset of {"profile":{"name":{}}}',
      ),
    );
  });

  it("throws CursorError when resolve query is a superset of the cursor resolveFingerprint", () => {
    const decodedCursor = buildCursor({
      resolveFingerprint: { profile: { name: true } },
    });
    const encoded = encodeCursor(decodedCursor);

    expect(() =>
      decodeAndValidateCursor({
        cursor: encoded,
        rootId,
        resolve: { profile: { name: true, email: true } },
      }),
    ).toThrowError(
      new CursorError(
        'Invalid cursor: resolve query mismatch. Expected {"profile":{"name":{},"email":{}}} to be a subset of {"profile":{"name":{}}}',
      ),
    );
  });

  it("throws CursorError when decoded payload does not match schema", () => {
    const invalidCursor: CoValueCursor = encodeCursor({
      // @ts-expect-error invalid version
      version: 2,
      rootId,
      resolveFingerprint: {},
      frontiers: {},
    });

    expect(() =>
      decodeAndValidateCursor({
        cursor: invalidCursor,
        rootId,
        resolve: {},
      }),
    ).toThrowError(new CursorError("Invalid cursor string"));
  });

  it("throws when cursor cannot be decoded as JSON", () => {
    expect(() =>
      decodeAndValidateCursor({
        cursor: "cursor_z3",
        rootId,
        resolve: {},
      }),
    ).toThrowError(new CursorError("Invalid cursor string"));
  });
});
