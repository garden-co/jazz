import type { DbConfig } from "./db.js";

const opaqueValueIds = new WeakMap<object, number>();
let nextOpaqueValueId = 0;

function opaqueValueId(value: object): number {
  let id = opaqueValueIds.get(value);
  if (id === undefined) {
    id = nextOpaqueValueId++;
    opaqueValueIds.set(value, id);
  }
  return id;
}

function isPlainRecord(value: object): value is Record<string, unknown> {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function canonicalizeConfigValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeConfigValue);
  }
  if (value === null || (typeof value !== "object" && typeof value !== "function")) {
    return value;
  }
  // Date is JSON-compatible, so preserve its JSON representation rather than
  // treating it as an opaque object with no enumerable properties.
  if (value instanceof Date) {
    return value.toJSON();
  }
  if (!isPlainRecord(value)) {
    // Runtime sources can contain values such as WebAssembly.Module,
    // MessagePort, ArrayBuffer, or other opaque objects. Their enumerable
    // shape does not describe their identity, so only the same reference may
    // share a client configuration.
    return { $jazzOpaqueValue: opaqueValueId(value) };
  }

  const canonical: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    canonical[key] = canonicalizeConfigValue(value[key]);
  }
  return canonical;
}

/** Stable structural identity for registry and framework config comparisons. */
export function serializeClientConfig(config: DbConfig): string {
  return JSON.stringify(canonicalizeConfigValue(config));
}
