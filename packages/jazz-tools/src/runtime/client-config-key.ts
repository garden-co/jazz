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

function encodeString(value: string): string {
  return `${value.length}:${value}`;
}

function canonicalizeConfigValue(value: unknown, active: WeakSet<object>): string | undefined {
  if (value === null) return "N";
  if (typeof value === "string") return `S${encodeString(value)}`;
  if (typeof value === "boolean") return value ? "T" : "F";
  if (typeof value === "number") {
    // Match JSON's treatment of non-finite numbers and negative zero.
    return Number.isFinite(value) ? `D${JSON.stringify(value)};` : "N";
  }
  if (typeof value === "undefined" || typeof value === "symbol") return undefined;
  if (typeof value === "bigint") {
    throw new TypeError("BigInt values are not supported in client configuration");
  }
  // Date is JSON-compatible, so preserve its JSON representation rather than
  // treating it as an opaque object with no enumerable properties.
  if (value instanceof Date) {
    const jsonValue = value.toJSON();
    return jsonValue === null ? "N" : `S${encodeString(jsonValue)}`;
  }
  if (!Array.isArray(value) && !isPlainRecord(value)) {
    // Runtime sources can contain values such as WebAssembly.Module,
    // MessagePort, ArrayBuffer, or other opaque objects. Their enumerable
    // shape does not describe their identity, so only the same reference may
    // share a client configuration.
    return `X${opaqueValueId(value)};`;
  }

  if (active.has(value)) {
    throw new TypeError("Cyclic values are not supported in client configuration");
  }
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const items: string[] = [];
      for (let index = 0; index < value.length; index++) {
        items.push(canonicalizeConfigValue(value[index], active) ?? "N");
      }
      return `A${items.length}:[${items.join("")}]`;
    }

    const entries: string[] = [];
    for (const key of Object.keys(value).sort()) {
      const encodedValue = canonicalizeConfigValue(value[key], active);
      // Match JSON object semantics: undefined and symbol-valued properties
      // do not contribute to structural identity.
      if (encodedValue !== undefined) {
        entries.push(`K${encodeString(key)}${encodedValue}`);
      }
    }
    return `O${entries.length}:{${entries.join("")}}`;
  } finally {
    active.delete(value);
  }
}

/** Stable structural identity for registry and framework config comparisons. */
export function serializeClientConfig(config: DbConfig): string {
  return canonicalizeConfigValue(config, new WeakSet())!;
}

/** Namespaced registry identity for a client configuration. */
export function createClientConfigKey(namespace: string, config: DbConfig): string {
  return `${namespace}:${serializeClientConfig(config)}`;
}
