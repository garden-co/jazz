/** Stable IDs, not display names. Unused optional fields encode as empty. */
export type CryptoContext = Readonly<{
  application: string;
  policy: string;
  scope: string;
  identifier: string;
  epoch: string;
  table?: string;
  row?: string;
  column?: string;
  recipient?: string;
}>;

const FIELDS = [
  "application",
  "policy",
  "scope",
  "identifier",
  "table",
  "row",
  "column",
  "epoch",
  "recipient",
] as const;
const FIELD_NAMES = new Set<string>(FIELDS);
const REQUIRED = new Set<string>(["application", "policy", "scope", "identifier", "epoch"]);

/** Canonical context format 1; semantic field selection belongs to the common E2EE layer. */
export function encodeCryptoContext(context: CryptoContext): Uint8Array {
  if (
    Reflect.ownKeys(context).some((field) => typeof field !== "string" || !FIELD_NAMES.has(field))
  ) {
    throw new Error("Unknown E2EE context field");
  }
  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const fields = FIELDS.map((field) => {
    const value = context[field] === undefined ? "" : context[field];
    if (
      typeof value !== "string" ||
      value.length > 65535 ||
      (REQUIRED.has(field) && value === "")
    ) {
      throw new Error("Invalid E2EE context field");
    }
    const bytes = encoder.encode(value);
    if (bytes.length > 65535 || decoder.decode(bytes) !== value) {
      throw new Error("Invalid E2EE context encoding");
    }
    return bytes;
  });
  const result = new Uint8Array(5 + fields.reduce((size, field) => size + 4 + field.length, 0));
  result.set([74, 69, 50, 67, 1]); // JE2C, context format version 1.
  const view = new DataView(result.buffer);
  let offset = 5;
  for (const field of fields) {
    view.setUint32(offset, field.length, false);
    result.set(field, offset + 4);
    offset += 4 + field.length;
  }
  return result;
}
