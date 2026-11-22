/**
 * `point` column values (SPEC 20.1): a WGS84 position in degrees.
 *
 * At the JS edge a point is `[lng, lat]`, GeoJSON order: longitude first,
 * although people often write latitude first. Inside the runtime it is
 * carried as a `Bytea` value holding its 16 `point-f64le/1` bytes: the
 * longitude, then the latitude, each an IEEE-754 binary64 little-endian.
 * Those are also its record payload and SQLite cell, so a point crosses
 * every binding (NAPI, WASM, React Native) as the same bytes.
 */

export const POINT_ENCODING = "point-f64le/1";
export const POINT_BYTES = 16;

/** A point at the JS edge: `[lng, lat]`. */
export type Point = readonly [lng: number, lat: number];

function where(column?: string): string {
  return column ? ` for column ${column}` : "";
}

function checkCoordinates(lng: number, lat: number, column?: string): void {
  if (!(lng >= -180 && lng <= 180) || !(lat >= -90 && lat <= 90)) {
    throw new Error(
      `Point [${lng}, ${lat}]${where(column)} is not [lng, lat] with a finite longitude in [-180, 180] and latitude in [-90, 90] (GeoJSON order: longitude first)`,
    );
  }
}

/**
 * Encode `[lng, lat]` as `point-f64le/1` bytes, refusing anything else and
 * non-finite or out-of-range coordinates (before the write reaches Rust,
 * which refuses them too).
 */
export function pointToBytes(value: unknown, column?: string): Uint8Array {
  if (isPointLiteralJson(value)) {
    const bytes = hexToBytes(value.$pointf64le);
    assertPointBytes(bytes, column);
    return bytes;
  }
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof value[0] !== "number" ||
    typeof value[1] !== "number"
  ) {
    throw new Error(`Expected a point [lng, lat] of two numbers${where(column)}`);
  }
  const [lng, lat] = value as [number, number];
  checkCoordinates(lng, lat, column);
  const bytes = new Uint8Array(POINT_BYTES);
  const view = new DataView(bytes.buffer);
  view.setFloat64(0, lng, true);
  view.setFloat64(8, lat, true);
  return bytes;
}

/** Check `point-f64le/1` bytes without keeping the decoded point. */
export function assertPointBytes(bytes: Uint8Array, column?: string): void {
  bytesToPoint(bytes, column);
}

/** Decode `point-f64le/1` bytes into `[lng, lat]` (any host endianness). */
export function bytesToPoint(bytes: Uint8Array, column?: string): Point {
  if (bytes.byteLength !== POINT_BYTES) {
    throw new Error(`Point${where(column)} has ${bytes.byteLength} bytes, expected ${POINT_BYTES}`);
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const lng = view.getFloat64(0, true);
  const lat = view.getFloat64(8, true);
  checkCoordinates(lng, lat, column);
  return [lng, lat];
}

/**
 * A point literal as query-builder JSON: the hex of its `point-f64le/1`
 * bytes. JSON numbers can't carry `-0`, and points compare bit for bit.
 */
export type PointLiteralJson = { $pointf64le: string };

export function pointLiteralJson(value: unknown, column?: string): PointLiteralJson {
  let hex = "";
  for (const byte of pointToBytes(value, column)) hex += byte.toString(16).padStart(2, "0");
  return { $pointf64le: hex };
}

function isPointLiteralJson(value: unknown): value is PointLiteralJson {
  return (
    typeof value === "object" &&
    value !== null &&
    Object.keys(value).length === 1 &&
    typeof (value as { $pointf64le?: unknown }).$pointf64le === "string"
  );
}

function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("Invalid $pointf64le point literal: expected hex of point-f64le/1 bytes");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
