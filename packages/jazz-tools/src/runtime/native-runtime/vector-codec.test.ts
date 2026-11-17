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
import { bytesToVector, vectorLiteralJson, vectorToBytes } from "../vector.js";
import { translateQuery } from "../query-adapter.js";
import { getSupportedWhereOperatorsForColumn } from "../../where-operators.js";

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const unhex = (text: string) => Uint8Array.from(Buffer.from(text.replaceAll(" ", ""), "hex"));

const vector2: ColumnDescriptor = {
  name: "v",
  column_type: { type: "Vector", dimensions: 2 },
  nullable: false,
};

// SPEC 20.1: a vector is `n` binary32 values in encoding `f32le/1`. These
// bytes are pinned by the Rust test
// `named_cell_input_carries_vectors_as_f32le_and_refuses_bad_ones`
// (crates/jazz/layers/db/src/binding_codec.rs): descriptor
// `[Some("v"): Vector(2)]` is `01 01 01 76 | 13 02`, then the record as a
// postcard byte string `08 | 00 00 80 3f 00 00 00 c0` ([1.0, -2.0]).
const NAMED_CELL_FIXTURE = "01 01 01 76 13 02 08 0000803f 000000c0";

describe("vector columns across the binding codec", () => {
  it("writes the named-cell envelope Rust pins, byte for byte", () => {
    const row = { v: toValue(new Float32Array([1, -2]), vector2.column_type) };
    expect(hex(encodeCellsForRow({ columns: [vector2] }, row))).toBe(
      hex(unhex(NAMED_CELL_FIXTURE)),
    );
  });

  it("round-trips ValueType::Vector (tag 19 + dimensions) and refuses bad dimensions", () => {
    const writer = new PostcardWriter();
    writeDescriptor(writer, [{ name: "v", valueType: storageColumnValueType(vector2) }]);
    const bytes = writer.finish();
    expect(hex(bytes)).toBe("0101017613" + "02");
    expect(readDescriptor(new PostcardReader(bytes))).toEqual([
      { name: "v", valueType: { tag: 19, dimensions: 2 } },
    ]);
    // 4096 is a two-byte varint; 0 and 4097 are refused on read.
    const max = new PostcardWriter();
    writeDescriptor(max, [{ name: "v", valueType: { tag: 19, dimensions: 4096 } }]);
    expect(hex(max.finish())).toBe("01010176" + "13" + "8020");
    expect(() => readDescriptor(new PostcardReader(unhex("01 01 01 76 13 00")))).toThrow(
      /vector dimensions 0/,
    );
    expect(() => readDescriptor(new PostcardReader(unhex("01 01 01 76 13 81 20")))).toThrow(
      /vector dimensions 4097/,
    );
  });

  it("stores the raw f32le payload (no stored-scalar prefix) and reads back a Float32Array", () => {
    const columns: ColumnDescriptor[] = [
      { name: "id", column_type: { type: "Integer" }, nullable: false },
      vector2,
      { name: "maybe", column_type: { type: "Vector", dimensions: 1 }, nullable: true },
      { name: "none", column_type: { type: "Vector", dimensions: 1 }, nullable: true },
    ];
    const values = [
      { type: "Integer" as const, value: 7 },
      toValue([1, -2], vector2.column_type),
      toValue(new Float32Array([0.5]), columns[2]!.column_type),
      { type: "Null" as const },
    ];
    const raw = encodeNativeRowValues(columns, values);
    // Fixed region (i32 7), two end offsets, then [1, -2], Some([0.5]), None.
    expect(hex(raw)).toBe(
      hex(unhex("07000000 14000000 19000000 0000803f 000000c0 01 0000003f 00")),
    );
    const decoded = decodeNativeRowValues(columns, raw);
    expect(decoded[1]).toEqual({ type: "Bytea", value: unhex("0000803f000000c0") });
    const vector = unwrapValue(decoded[1]!, vector2.column_type);
    expect(vector).toBeInstanceOf(Float32Array);
    expect(Array.from(vector as Float32Array)).toEqual([1, -2]);
    expect(decoded[3]).toEqual({ type: "Null" });
  });

  it("refuses a wrong length or a non-finite element before the write reaches Rust", () => {
    expect(() => toValue(new Float32Array([1]), vector2.column_type)).toThrow(
      /1 elements, expected 2/,
    );
    expect(() => toValue([1, Number.NaN], vector2.column_type)).toThrow(/element 1/);
    expect(() => toValue([Infinity, 0], vector2.column_type)).toThrow(/element 0/);
    // Finite in f64, infinite once rounded to f32.
    expect(() => toValue([1e39, 0], vector2.column_type)).toThrow(/float32 range/);
    expect(() => toValue("1,2", vector2.column_type)).toThrow(/Float32Array/);
    expect(() =>
      encodeNativeRowValues([vector2], [{ type: "Bytea", value: unhex("0000803f") }]),
    ).toThrow(/4 bytes, expected 8/);
    expect(() => decodeNativeRowValues([vector2], unhex("0000803f 0000c07f"))).toThrow(/element 1/);
  });

  it("decodes little-endian bits exactly, including -0, subnormals and f32 max", () => {
    const input = new Float32Array([-0, 1e-45, 3.4028234663852886e38]);
    const bytes = vectorToBytes(input, 3);
    expect(hex(bytes)).toBe("00000080" + "01000000" + "ffff7f7f");
    const output = bytesToVector(bytes);
    expect(Object.is(output[0], -0)).toBe(true);
    expect(Array.from(output)).toEqual(Array.from(input));
  });
});

describe("vector filters compare whole values, bit for bit (SPEC 6.1.1)", () => {
  const schema = {
    docs: {
      columns: [
        { name: "embedding", column_type: { type: "Vector", dimensions: 2 }, nullable: true },
      ],
    },
  } as never;
  const conditions = (op: string, value: unknown) =>
    JSON.parse(
      translateQuery(
        JSON.stringify({ table: "docs", conditions: [{ column: "embedding", op, value }] }),
        schema,
      ),
    ).conditions;

  it("encodes eq/ne/in literals as their f32le bytes, keeping -0 distinct from 0", () => {
    // The builder carries a Float32Array as hex of its bytes: JSON numbers lose -0.
    const literal = vectorLiteralJson(new Float32Array([-0, 1]));
    expect(literal).toEqual({ $f32le: "00000080" + "0000803f" });
    const [eq] = conditions("eq", literal);
    expect(JSON.stringify(eq)).toContain(JSON.stringify([0, 0, 0, 128, 0, 0, 128, 63]));
    const [plain] = conditions("eq", [0, 1]);
    expect(JSON.stringify(plain)).toContain(JSON.stringify([0, 0, 0, 0, 0, 0, 128, 63]));
    expect(() => conditions("ne", literal)).not.toThrow();
    expect(() => conditions("in", [literal, [1, 2]])).not.toThrow();
    expect(() => conditions("isNull", true)).not.toThrow();
  });

  it("refuses ordering, contains, a wrong length and non-finite literals", () => {
    for (const op of ["gt", "gte", "lt", "lte", "contains"]) {
      expect(() => conditions(op, [1, 2])).toThrow(/only supports eq\/ne\/in\/notIn\/isNull/);
    }
    expect(() => conditions("eq", [1, 2, 3])).toThrow(/3 elements, expected 2/);
    expect(() => conditions("eq", { $f32le: "0000c07f0000803f" })).toThrow(/element 0/);
  });

  it("offers the whole-value operators bytes have", () => {
    expect(
      getSupportedWhereOperatorsForColumn({
        name: "embedding",
        columnType: { type: "Vector", dimensions: 2 },
        nullable: true,
      }),
    ).toEqual(["eq", "ne", "in", "notIn", "isNull"]);
  });
});
