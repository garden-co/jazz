import type { FFIRecord, FFIRow, FFIValue, Value } from "../drivers/types.js";

type JsonFFIValue =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: number }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: string }
  | { type: "Array"; value: JsonFFIValue[] }
  | { type: "Row"; value: { id?: string; values: JsonFFIValue[] } }
  | { type: "Null" };

type JsonFFIRecord = Record<string, JsonFFIValue>;
type JsonFFIRow = {
  id: string;
  values: JsonFFIValue[];
};

function encodeHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function decodeHex(value: string): Uint8Array {
  if (value.length % 2 !== 0) {
    throw new Error("Invalid Bytea hex payload: expected an even-length string");
  }

  const bytes = new Uint8Array(value.length / 2);
  for (let i = 0; i < value.length; i += 2) {
    const byte = Number.parseInt(value.slice(i, i + 2), 16);
    if (!Number.isFinite(byte)) {
      throw new Error("Invalid Bytea hex payload: expected only hexadecimal characters");
    }
    bytes[i / 2] = byte;
  }
  return bytes;
}

export function toFFIValue(value: Value): FFIValue {
  switch (value.type) {
    case "Bytea":
      return { type: "Bytea", value: new Uint8Array(value.value) };
    case "Array":
      return { type: "Array", value: value.value.map((entry) => toFFIValue(entry)) };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) => toFFIValue(entry)),
        },
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Null":
      return { ...value };
  }
}

export function toFFIRecord(values: Record<string, Value>): FFIRecord {
  return Object.fromEntries(Object.entries(values).map(([key, value]) => [key, toFFIValue(value)]));
}

function encodeJsonFFIValue(value: FFIValue): JsonFFIValue {
  switch (value.type) {
    case "Bytea":
      return { type: "Bytea", value: encodeHex(value.value) };
    case "Array":
      return { type: "Array", value: value.value.map((entry) => encodeJsonFFIValue(entry)) };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) => encodeJsonFFIValue(entry)),
        },
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Null":
      return { ...value };
  }
}

function decodeJsonFFIValue(value: JsonFFIValue): FFIValue {
  switch (value.type) {
    case "Bytea":
      return { type: "Bytea", value: decodeHex(value.value) };
    case "Array":
      return { type: "Array", value: value.value.map((entry) => decodeJsonFFIValue(entry)) };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) => decodeJsonFFIValue(entry)),
        },
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Null":
      return { ...value };
  }
}

export function encodeFFIRecordToJson(values: FFIRecord): string {
  const jsonValues: JsonFFIRecord = Object.fromEntries(
    Object.entries(values).map(([key, value]) => [key, encodeJsonFFIValue(value)]),
  );
  return JSON.stringify(jsonValues);
}

export function decodeFFIRowFromJson(json: string): FFIRow {
  const parsed = JSON.parse(json) as JsonFFIRow;
  return {
    id: parsed.id,
    values: parsed.values.map((value) => decodeJsonFFIValue(value)),
  };
}
