// Durable Groove encoding of policy claims in the policy-binding directory.
import { SortedMap, cmpStr } from "../../support/collections.js";
import {
  RecordDescriptor,
  OwnedRecord,
  EnumSchema,
  EnumCase,
  EnumValue,
  ScalarEnumSchema,
  type GValue,
  type GValueType,
} from "../../_extern/model-core.js";
import type { Claims } from "./policy_claims.js";

const MAX_NODES = 1024;

const K = {
  U8: 0,
  U16: 1,
  U32: 2,
  U64: 3,
  I32: 4,
  I64: 5,
  F64: 6,
  BOOL: 7,
  STRING: 8,
  BYTES: 9,
  UUID: 10,
  ENUM_TAG: 11,
  NULL: 12,
  TUPLE: 13,
  ARRAY: 14,
  NULLABLE: 15,
} as const;
const KIND_NAMES = [
  "u8",
  "u16",
  "u32",
  "u64",
  "i32",
  "i64",
  "f64",
  "bool",
  "string",
  "bytes",
  "uuid",
  "enum_tag",
  "null",
  "tuple",
  "array",
  "nullable",
];
const SCALAR_KINDS: Record<string, number> = {
  U8: K.U8,
  U16: K.U16,
  U32: K.U32,
  U64: K.U64,
  I32: K.I32,
  I64: K.I64,
  F64: K.F64,
  Bool: K.BOOL,
  String: K.STRING,
  Bytes: K.BYTES,
  Uuid: K.UUID,
};
const SCALAR_TYPES: GValueType[] = [
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "F64",
  "Bool",
  "String",
  "Bytes",
  "Uuid",
  "U8",
].map((kind) => ({ kind }));

/** Flatten claims into Groove records (pre-order, children by position). */
export function policyBindingDirectoryClaimsValue(claims: Claims): GValue {
  const nodes: GValue[] = [];
  for (const [name, value] of claims) encodeNode(nodes, name, value);
  return { kind: "Array", value: nodes };
}

export function policyBindingDirectoryClaimsFromValue(value: GValue): Claims {
  if (value.kind !== "Array") throw new Error("policy binding directory claims must be an array");
  const nodes: GValue[] = value.value;
  if (nodes.length > MAX_NODES)
    throw new Error("policy binding directory claims exceed node limit");
  const cursor = { i: 0 };
  const claims: Claims = new SortedMap(cmpStr);
  while (cursor.i < nodes.length) {
    const [name, v] = decodeNode(nodes, cursor, true);
    if (name === undefined) throw new Error("policy binding directory root claim is unnamed");
    if (claims.set(name, v) !== undefined)
      throw new Error("policy binding directory contains duplicate claim names");
  }
  return claims;
}

export function policyDirectoryDescriptor(): any {
  return new RecordDescriptor([
    ["derived_v1", { kind: "U8" }],
    ["claims_v1", { kind: "Array", inner: { kind: "Record", descriptor: nodeDescriptor() } }],
  ]);
}

export function policyDirectoryPayload(presence: number, claims: GValue): GValue {
  const d = policyDirectoryDescriptor();
  let raw;
  try {
    raw = d.create([{ kind: "U8", value: presence }, claims]);
  } catch (e) {
    throw new Error(String(e));
  }
  return { kind: "Record", value: new OwnedRecord(raw, d) };
}

export const policyBindingDirectoryClaimsValueType = (): GValueType => ({
  kind: "Record",
  descriptor: policyDirectoryDescriptor(),
});

let valueSchema: any;
function policyClaimValueSchema(): any {
  if (valueSchema) return valueSchema;
  const empty = () => new RecordDescriptor([]);
  const cases = KIND_NAMES.map((name, tag) =>
    tag < SCALAR_TYPES.length
      ? new EnumCase(name, new RecordDescriptor([["value", SCALAR_TYPES[tag]]]))
      : new EnumCase(name, empty()),
  );
  return (valueSchema = new EnumSchema("jazz.internal.policy_claim_directory_value.v1", cases));
}

let nodeDesc: any;
function nodeDescriptor(): any {
  return (nodeDesc ??= new RecordDescriptor([
    ["name", { kind: "Nullable", inner: { kind: "String" } }],
    [
      "kind",
      {
        kind: "EnumTag",
        schema: new ScalarEnumSchema(
          "jazz.internal.policy_claim_directory_node_kind.v1",
          KIND_NAMES,
        ),
      },
    ],
    ["children", { kind: "U32" }],
    ["value", { kind: "Enum", schema: policyClaimValueSchema() }],
  ]));
}

function enumValue(tag: number, payload: GValue[], what: string): any {
  try {
    return EnumValue.create(tag, policyClaimValueSchema().case(tag).payload, payload);
  } catch (e) {
    throw new Error(`policy binding directory ${what} is invalid: ${e}`);
  }
}

function encodeNode(nodes: GValue[], name: string | undefined, value: GValue): void {
  if (nodes.length >= MAX_NODES)
    throw new Error("policy binding directory claims exceed node limit");
  let kind: number;
  let ev: any;
  let children: GValue[] = [];
  if (value.kind in SCALAR_KINDS) {
    kind = SCALAR_KINDS[value.kind];
    ev = enumValue(kind, [value], "scalar");
  } else
    switch (value.kind) {
      case "EnumTag":
        kind = K.ENUM_TAG;
        ev = enumValue(kind, [{ kind: "U8", value: value.value }], "scalar");
        break;
      case "Nullable":
        kind = value.value === undefined ? K.NULL : K.NULLABLE;
        children = value.value === undefined ? [] : [value.value];
        ev = enumValue(kind, [], "container");
        break;
      case "Tuple":
      case "Array":
        kind = value.kind === "Tuple" ? K.TUPLE : K.ARRAY;
        children = value.value;
        ev = enumValue(kind, [], "container");
        break;
      default:
        throw new Error("policy binding directory does not admit engine-owned claim values");
    }
  if (children.length > 0xffffffff)
    throw new Error("policy binding directory has too many child claims");
  const d = nodeDescriptor();
  let raw;
  try {
    raw = d.create([
      { kind: "Nullable", value: name === undefined ? undefined : { kind: "String", value: name } },
      { kind: "EnumTag", value: kind },
      { kind: "U32", value: children.length },
      { kind: "Enum", value: ev },
    ]);
  } catch (e) {
    throw new Error(`policy binding directory claim node is invalid: ${e}`);
  }
  nodes.push({ kind: "Record", value: new OwnedRecord(raw, d) });
  for (const c of children) encodeNode(nodes, undefined, c);
}

function decodeNode(
  nodes: GValue[],
  cursor: { i: number },
  root: boolean,
): [string | undefined, GValue] {
  const node = nodes[cursor.i];
  if (node === undefined) throw new Error("policy binding directory claim tree ended early");
  cursor.i++;
  if (node.kind !== "Record") throw new Error("policy binding directory node must be a record");
  const record = node.value;
  if (!record.descriptor().equals(nodeDescriptor()))
    throw new Error("policy binding directory node has unexpected descriptor");
  let values: GValue[];
  try {
    values = record.toValues();
  } catch (e) {
    throw new Error(`policy binding directory node cannot decode: ${e}`);
  }
  const [nameV, kindV, childrenV, enumV] = values;
  if (
    values.length !== 4 ||
    kindV.kind !== "EnumTag" ||
    childrenV.kind !== "U32" ||
    enumV.kind !== "Enum"
  )
    throw new Error("policy binding directory node has invalid fields");
  let name: string | undefined;
  if (nameV.kind !== "Nullable")
    throw new Error("policy binding directory name must be nullable string");
  if (nameV.value !== undefined) {
    if (nameV.value.kind !== "String")
      throw new Error("policy binding directory name must be string");
    name = nameV.value.value;
  }
  if (root !== (name !== undefined))
    throw new Error(
      root
        ? "policy binding directory root claim is unnamed"
        : "policy binding directory child claim is named",
    );
  const tag: number = kindV.value;
  if (enumV.value.tag() !== tag || tag > K.NULLABLE)
    throw new Error("policy binding directory kind and value disagree");
  let payload: GValue[];
  try {
    payload = enumV.value.record().toValues();
  } catch (e) {
    throw new Error(`policy binding directory value cannot decode: ${e}`);
  }
  const childCount: number = childrenV.value;
  const scalar = () => {
    if (childCount !== 0 || payload.length !== 1)
      throw new Error("policy binding directory scalar has invalid children or payload");
    return payload[0];
  };
  let value: GValue;
  if (tag <= K.UUID) value = scalar();
  else if (tag === K.ENUM_TAG) {
    const s = scalar();
    if (s.kind !== "U8") throw new Error("policy binding directory enum tag must be u8");
    value = { kind: "EnumTag", value: s.value };
  } else if (tag === K.NULL) {
    if (childCount !== 0 || payload.length)
      throw new Error("policy binding directory null has payload or children");
    value = { kind: "Nullable", value: undefined };
  } else {
    if (payload.length) throw new Error("policy binding directory container has payload");
    if (tag === K.NULLABLE && childCount !== 1)
      throw new Error("policy binding directory nullable must have one child");
    const children: GValue[] = [];
    for (let i = 0; i < childCount; i++) children.push(decodeNode(nodes, cursor, false)[1]);
    value =
      tag === K.TUPLE
        ? { kind: "Tuple", value: children }
        : tag === K.ARRAY
          ? { kind: "Array", value: children }
          : { kind: "Nullable", value: children[0] };
  }
  return [name, value];
}
