import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  readDescriptor,
  storageColumnTypeToValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import { encodeSchema } from "./schema-codec.js";

describe("native row and schema value-type codecs", () => {
  it("keeps Jazz JSON schema tag 15 separate from Groove I32 descriptor tag 15", () => {
    const schema = encodeSchema({
      documents: {
        columns: [
          {
            name: "payload",
            column_type: { type: "Json", schema: { type: "object" } },
            nullable: false,
          },
        ],
      },
    });
    const schemaReader = new PostcardReader(schema);
    expect(schemaReader.u64()).toBe(1);
    expect(schemaReader.string()).toBe("documents");
    expect(schemaReader.u64()).toBe(1);
    expect(schemaReader.string()).toBe("payload");
    expect(schemaReader.u64()).toBe(15);
    expect(schemaReader.option((reader) => reader.string())).toBe('{"type":"object"}');

    expect(storageColumnTypeToValueType({ type: "Integer" })).toEqual({ tag: 15 });
    expect(storageColumnTypeToValueType({ type: "BigInt" })).toEqual({ tag: 14 });

    const descriptorWriter = new PostcardWriter();
    writeDescriptor(descriptorWriter, [
      { name: "count", valueType: storageColumnTypeToValueType({ type: "Integer" }) },
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
    ]);
    descriptorWriter.u64(42);
    const descriptorReader = new PostcardReader(descriptorWriter.finish());
    expect(readDescriptor(descriptorReader)).toEqual([
      { name: "count", valueType: { tag: 15 } },
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
    ]);
    expect(descriptorReader.u64()).toBe(42);
  });
});
