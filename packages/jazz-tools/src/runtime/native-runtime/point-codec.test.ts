import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import { encodeCellsForRow } from "./native-runtime-adapter.js";
import {
  decodeNativeRowValues,
  encodeNativeRowValues,
  readDescriptor,
  storageColumnValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import type { ColumnDescriptor } from "../../drivers/types.js";
import { toValue } from "../value-converter.js";
import { unwrapValue } from "../row-transformer.js";
import { bytesToPoint, pointLiteralJson, pointToBytes } from "../point.js";
import { translateQuery } from "../query-adapter.js";
import { getSupportedWhereOperatorsForColumn } from "../../where-operators.js";

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const unhex = (text: string) => Uint8Array.from(Buffer.from(text.replaceAll(" ", ""), "hex"));

const location: ColumnDescriptor = {
  name: "p",
  column_type: { type: "Point" },
  nullable: false,
};

// SPEC 20.1: a point is `[lng, lat]` in encoding `point-f64le/1`. These
// bytes are pinned by the Rust test
// `named_cell_input_carries_points_as_f64le_and_refuses_bad_ones`
// (crates/jazz/layers/db/src/binding_codec.rs): descriptor
// `[Some("p"): Point]` is `01 01 01 70 | 14`, then the record as a postcard
// byte string `10 | 000000000000f03f 00000000000000c0` ([1.0, -2.0]).
const NAMED_CELL_FIXTURE = "01 01 01 70 14 10 000000000000f03f 00000000000000c0";

describe("point columns across the binding codec", () => {
  it("writes the named-cell envelope Rust pins, byte for byte", () => {
    const row = { p: toValue([1, -2], location.column_type) };
    expect(hex(encodeCellsForRow({ columns: [location] }, row))).toBe(
      hex(unhex(NAMED_CELL_FIXTURE)),
    );
  });

  it("round-trips ValueType::Point (tag 20, no payload)", () => {
    const writer = new PostcardWriter();
    writeDescriptor(writer, [{ name: "p", valueType: storageColumnValueType(location) }]);
    const bytes = writer.finish();
    expect(hex(bytes)).toBe("0101017014");
    expect(readDescriptor(new PostcardReader(bytes))).toEqual([
      { name: "p", valueType: { tag: 20 } },
    ]);
  });

  it("stores the raw point-f64le payload (no stored-scalar prefix) and reads back [lng, lat]", () => {
    const columns: ColumnDescriptor[] = [
      { name: "id", column_type: { type: "Integer" }, nullable: false },
      location,
      { name: "maybe", column_type: { type: "Point" }, nullable: true },
      { name: "none", column_type: { type: "Point" }, nullable: true },
    ];
    const values = [
      { type: "Integer" as const, value: 7 },
      toValue([1, -2], location.column_type),
      toValue([0.5, 0.25], columns[2]!.column_type),
      { type: "Null" as const },
    ];
    const raw = encodeNativeRowValues(columns, values);
    // Fixed region (i32 7), two end offsets, then [1, -2], Some([0.5, 0.25]), None.
    expect(hex(raw)).toBe(
      hex(
        unhex(
          "07000000 1c000000 2d000000 000000000000f03f 00000000000000c0 01 000000000000e03f 000000000000d03f 00",
        ),
      ),
    );
    const decoded = decodeNativeRowValues(columns, raw);
    expect(decoded[1]).toEqual({
      type: "Bytea",
      value: unhex("000000000000f03f00000000000000c0"),
    });
    expect(unwrapValue(decoded[1]!, location.column_type)).toEqual([1, -2]);
    expect(decoded[3]).toEqual({ type: "Null" });
  });

  it("refuses a wrong shape, non-finite or out-of-range coordinates before the write reaches Rust", () => {
    const type = location.column_type;
    expect(() => toValue([1], type)).toThrow(/two numbers/);
    expect(() => toValue([1, 2, 3], type)).toThrow(/two numbers/);
    expect(() => toValue({ lng: 1, lat: 2 }, type)).toThrow(/two numbers/);
    expect(() => toValue([Number.NaN, 0], type)).toThrow(/longitude in \[-180, 180\]/);
    expect(() => toValue([0, Infinity], type)).toThrow(/latitude in \[-90, 90\]/);
    expect(() => toValue([180.0000001, 0], type)).toThrow(/GeoJSON order/);
    // Latitude first by mistake: San Francisco as [lat, lng].
    expect(() => toValue([37.774929, -122.419416], type)).toThrow(/longitude first/);
    expect(() =>
      encodeNativeRowValues([location], [{ type: "Bytea", value: unhex("0000803f") }]),
    ).toThrow(/4 bytes, expected 16/);
    expect(() =>
      decodeNativeRowValues([location], unhex("0000000000000000 0000000000006040")),
    ).toThrow(/latitude/);
  });

  it("decodes little-endian bits exactly, including -0, subnormals and the range edges", () => {
    const bytes = pointToBytes([-0, Number.MIN_VALUE]);
    expect(hex(bytes)).toBe("0000000000000080" + "0100000000000000");
    const [lng, lat] = bytesToPoint(bytes);
    expect(Object.is(lng, -0)).toBe(true);
    expect(lat).toBe(Number.MIN_VALUE);
    expect(bytesToPoint(pointToBytes([-180, 90]))).toEqual([-180, 90]);
  });
});

describe("point filters compare whole values, bit for bit (SPEC 6.1.1)", () => {
  const schema = {
    places: {
      columns: [{ name: "location", column_type: { type: "Point" }, nullable: true }],
    },
  } as never;
  const conditions = (op: string, value: unknown) =>
    JSON.parse(
      translateQuery(
        JSON.stringify({ table: "places", conditions: [{ column: "location", op, value }] }),
        schema,
      ),
    ).conditions;

  it("encodes eq/ne/in literals as their point-f64le bytes, keeping -0 distinct from 0", () => {
    // The builder carries a point as hex of its bytes: JSON numbers lose -0.
    const literal = pointLiteralJson([-0, 1]);
    expect(literal).toEqual({ $pointf64le: "0000000000000080" + "000000000000f03f" });
    const [eq] = conditions("eq", literal);
    expect(JSON.stringify(eq)).toContain(
      JSON.stringify([0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 240, 63]),
    );
    const [plain] = conditions("eq", [0, 1]);
    expect(JSON.stringify(plain)).toContain(
      JSON.stringify([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63]),
    );
    expect(() => conditions("ne", literal)).not.toThrow();
    expect(() => conditions("in", [literal, [1, 2]])).not.toThrow();
    expect(() => conditions("isNull", true)).not.toThrow();
  });

  it("leaves which operators a point takes to the engine (jazz#5966)", () => {
    for (const op of ["gt", "gte", "lt", "lte", "contains"]) {
      expect(() => conditions(op, [1, 2])).not.toThrow();
    }
  });

  it("refuses out-of-range literals", () => {
    expect(() => conditions("eq", [1, 91])).toThrow(/latitude/);
    expect(() => conditions("eq", { $pointf64le: "00" })).toThrow(/1 bytes, expected 16/);
  });

  it("offers the whole-value operators bytes have", () => {
    expect(
      getSupportedWhereOperatorsForColumn({
        name: "location",
        columnType: { type: "Point" },
        nullable: true,
      }),
    ).toEqual(["eq", "ne", "in", "notIn", "isNull"]);
  });
});
