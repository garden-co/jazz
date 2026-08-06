import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  createRecord,
  decodeRecordBool,
  decodeRecordBytes,
  decodeRecordString,
  decodeRecordValue,
  readDescriptor,
  writeDescriptor,
} from "./native-row-codec.js";

type NativeRowCodecFixture = {
  descriptor_hex: string;
  record_hex: string;
};

describe("native row codec", () => {
  it("round-trips the Record descriptor payload before reading the next field", () => {
    const writer = new PostcardWriter();
    writeDescriptor(writer, [
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
      { name: "count", valueType: { tag: 15 } },
    ]);
    writer.u64(42);

    const reader = new PostcardReader(writer.finish());
    expect(readDescriptor(reader)).toEqual([
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
      { name: "count", valueType: { tag: 15 } },
    ]);
    expect(reader.u64()).toBe(42);
  });

  it("decodes the Rust I32/I64 descriptor fixture without treating fixed fields as offsets", () => {
    const fixture = nativeRowCodecFixture();
    const descriptorBytes = hexToBytes(fixture.descriptor_hex);
    const raw = hexToBytes(fixture.record_hex);
    const descriptor = readDescriptor(new PostcardReader(descriptorBytes));

    expect(descriptor.map((field) => [field.name, field.valueType.tag])).toEqual([
      ["row_uuid", 8],
      ["user_title", 12],
      ["user_done", 12],
      ["user_priority", 12],
      ["tx_time", 14],
      ["user_description", 12],
    ]);
    expect(descriptor[3]?.valueType.inner?.tag).toBe(15);

    expect(decodeRecordString(descriptor, raw, 1)).toBe("Buy milk");
    expect(decodeRecordBool(descriptor, raw, 2)).toBe(false);
    expect(decodeRecordBytes(descriptor, raw, 3)).toEqual(Uint8Array.of(7, 0, 0, 0));
    expect(decodeRecordBytes(descriptor, raw, 4)).toEqual(Uint8Array.of(42, 0, 0, 0, 0, 0, 0, 0));
    expect(decodeRecordValue(descriptor, raw, 5)).toBeNull();

    const descriptorWriter = new PostcardWriter();
    writeDescriptor(descriptorWriter, descriptor);
    expect(descriptorWriter.finish()).toEqual(descriptorBytes);
    expect(
      createRecord(descriptor, [
        raw.subarray(0, 16),
        Uint8Array.of(1, ...new TextEncoder().encode("Buy milk")),
        Uint8Array.of(1, 0),
        Uint8Array.of(1, 7, 0, 0, 0),
        Uint8Array.of(42, 0, 0, 0, 0, 0, 0, 0),
        Uint8Array.of(0),
      ]),
    ).toEqual(raw);
  });
});

function nativeRowCodecFixture(): NativeRowCodecFixture {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/native_row_codec.json", import.meta.url),
      "utf8",
    ),
  ) as NativeRowCodecFixture;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
