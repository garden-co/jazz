import type { Db, TableProxy } from "../runtime/db.js";
import type { Value } from "../drivers/types.js";
import { encodeNativeRowValues } from "../runtime/native-runtime/native-row-codec.js";
import { encryptedSchemas } from "./encrypted-schema.js";
import { dataContext } from "./cell-data.js";
import { cellCryptoForDb, equalityCryptoForDb } from "./lifecycle.js";
import { decodeEnvelope } from "./envelope.js";
import { E2eeDataError } from "./data-error.js";
import type { SpaceRoot } from "./spaces.js";

function canonicalEqualityValue(value: Value): Value {
  if (value.type === "Double" && value.value === 0) return { ...value, value: 0 };
  if (value.type === "Array") return { ...value, value: value.value.map(canonicalEqualityValue) };
  if (value.type === "Enum")
    return {
      ...value,
      value: { ...value.value, values: value.value.values.map(canonicalEqualityValue) },
    };
  return value;
}

export function equalityValue(table: TableProxy<unknown, unknown>, name: string, value: Value) {
  const column = encryptedSchemas
    .get(table._schema)
    ?.logical[table._table]?.columns.find((column) => column.name === name);
  if (!column) throw new Error("Encrypted index declaration is unavailable");
  // Numeric equality identifies both signed zeros; cell encryption still preserves the value.
  return encodeNativeRowValues([{ ...column, name: "value" }], [canonicalEqualityValue(value)]);
}

/** Row-independent, but bound to the stable table, column, space and epoch. */
export async function equalityToken(
  db: Db,
  table: TableProxy<unknown, unknown>,
  name: string,
  value: Value,
  secret: Uint8Array,
  root: Readonly<SpaceRoot>,
): Promise<Uint8Array> {
  const column = encryptedSchemas
    .get(table._schema)
    ?.logical[table._table]?.columns.find((column) => column.name === name);
  if (!column) throw new Error("Encrypted index declaration is unavailable");
  const { application } = await cellCryptoForDb(db);
  const index = await equalityCryptoForDb(db);
  const context = await dataContext(
    db,
    table,
    undefined,
    column,
    root,
    application,
    "jazz.e2ee.equality.v1",
  );
  const plaintext = equalityValue(table, name, value);
  try {
    const token = await index.token(secret, context, plaintext);
    decodeEnvelope(index.mechanism, token);
    return Uint8Array.from(token);
  } catch {
    throw new E2eeDataError("encryption-failed");
  } finally {
    plaintext.fill(0);
  }
}
