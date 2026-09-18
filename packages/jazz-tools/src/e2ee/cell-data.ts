import type { ColumnDescriptor, Value } from "../drivers/types.js";
import type { Db, TableProxy } from "../runtime/db.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import {
  encodeNativeRowValues,
  decodeNativeRowValues,
} from "../runtime/native-runtime/native-row-codec.js";
import { unwrapValue } from "../runtime/row-transformer.js";
import { encodeCellType } from "./cell-type.js";
import { E2eeDataError } from "./data-error.js";
import { encodeCryptoContext } from "./context.js";
import { encodeEnvelope, decodeEnvelope } from "./envelope.js";
import { encryptedRowSpaces, encryptedSchemas } from "./encrypted-schema.js";
import { frameCryptoRecord } from "./record-frame.js";
import { cellCryptoForDb, withSpaceKeys } from "./lifecycle.js";
import type { SpaceRoot } from "./spaces.js";
import { equalityValue } from "./equality-data.js";

const RECORD = { id: "jazz.e2ee.cell-record", version: 1 } as const;
// Owned comparison bytes: public decoding may discard JSON spelling or expose mutable arrays.
export const decryptedIndexBytes = new WeakMap<object, Map<string, Uint8Array>>();
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function storedCell(epoch: string, ciphertext: Uint8Array): Uint8Array {
  if (!UUID.test(epoch)) throw new Error("Invalid encrypted cell epoch");
  const payload = new Uint8Array(36 + ciphertext.length);
  payload.set(new TextEncoder().encode(epoch));
  payload.set(ciphertext, 36);
  return encodeEnvelope(RECORD, payload);
}

function readStoredCell(value: unknown): { epoch: string; ciphertext: Uint8Array } {
  if (!(value instanceof Uint8Array)) throw new E2eeDataError("invalid-ciphertext");
  const payload = readEnvelope(RECORD, value);
  if (payload.length < 47) throw new E2eeDataError("invalid-ciphertext");
  const epoch = new TextDecoder().decode(payload.subarray(0, 36));
  if (!UUID.test(epoch)) throw new E2eeDataError("invalid-ciphertext");
  return { epoch, ciphertext: payload.subarray(36) };
}

function readEnvelope(mechanism: Parameters<typeof decodeEnvelope>[0], value: Uint8Array) {
  try {
    return decodeEnvelope(mechanism, value);
  } catch (error) {
    // These are the fixed routing errors from our decoder, not adapter exceptions.
    throw new E2eeDataError(
      error instanceof Error && error.message === "Unsupported E2EE mechanism"
        ? "unsupported-format"
        : "invalid-ciphertext",
    );
  }
}

export async function dataContext(
  db: Db,
  table: TableProxy<unknown, unknown>,
  rowId: string | undefined,
  column: ColumnDescriptor,
  root: Readonly<SpaceRoot>,
  application: string,
  policy = "jazz.e2ee.cell-record.v1",
): Promise<Uint8Array> {
  if (rowId !== undefined && !UUID.test(rowId)) throw new Error("Invalid encrypted cell row ID");
  const tableId = await db.tableIdentity(table, true);
  const columnId = await db.columnIdentity(table, column.name, true);
  if (!tableId || !columnId) throw new Error("Encrypted column identity is unavailable");
  return frameCryptoRecord([
    encodeCryptoContext({
      application,
      policy,
      scope: root.scopeId,
      identifier: root.identifier,
      table: tableId,
      row: rowId,
      column: columnId,
      epoch: root.epochId,
    }),
    encodeCellType(column),
  ]);
}

export async function encryptCell(
  db: Db,
  table: TableProxy<unknown, unknown>,
  rowId: string,
  name: string,
  value: Value,
  secret: Uint8Array,
  root: Readonly<SpaceRoot>,
): Promise<Uint8Array> {
  const column = encryptedSchemas
    .get(table._schema)
    ?.logical[table._table]?.columns.find((column) => column.name === name);
  if (!column) throw new Error("Encrypted column declaration is unavailable");
  const { cipher, application } = await cellCryptoForDb(db);
  const aad = await dataContext(db, table, rowId, column, root, application);
  const plaintext = encodeNativeRowValues([{ ...column, name: "value" }], [value]);
  try {
    const ciphertext = await cipher.encrypt(secret, aad, plaintext);
    decodeEnvelope(cipher.mechanism, ciphertext);
    return storedCell(root.epochId, ciphertext);
  } catch {
    throw new E2eeDataError("encryption-failed");
  } finally {
    plaintext.fill(0);
  }
}

export async function decryptCellRows(
  db: Db,
  schema: TableProxy<unknown, unknown>["_schema"],
  tableName: string,
  rows: Record<string, unknown>[],
  initialSpaceKeys?: ReadonlyMap<string, { secret: Uint8Array; root: SpaceRoot }>,
): Promise<Record<string, unknown>[]> {
  const metadata = encryptedSchemas.get(schema);
  const declaration = metadata?.tables.get(tableName);
  if (!metadata || !declaration || !rows.length) return rows;
  if (!rows.some((row) => declaration.columns.some((name) => Object.hasOwn(row, name)))) {
    return rows;
  }
  const table = new TypedTableQueryBuilder(tableName, schema);
  const scope = new TypedTableQueryBuilder(declaration.scope, schema);
  const { cipher, application } = await cellCryptoForDb(db);
  const result: Record<string, unknown>[] = [];
  for (const row of rows) {
    const identifier = encryptedRowSpaces.get(row) ?? row[declaration.space];
    if (typeof identifier !== "string" || typeof row.id !== "string")
      throw new Error("Encrypted result is missing its row or space identity");
    const decoded = { ...row };
    const pending = new Map(
      declaration.columns
        .filter((name) => Object.hasOwn(row, name))
        .map((name) => [name, readStoredCell(row[name])]),
    );
    const decrypt = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
      for (const [name, cell] of pending) {
        if (cell.epoch !== root.epochId) continue;
        const column = metadata.logical[tableName]!.columns.find((column) => column.name === name)!;
        const aad = await dataContext(db, table, row.id as string, column, root, application);
        readEnvelope(cipher.mechanism, cell.ciphertext);
        let plaintext: Uint8Array | undefined;
        try {
          plaintext = await cipher.decrypt(secret, aad, cell.ciphertext);
          // Buffer.slice() aliases its input. Use a plain view so the codec's
          // byte-value slices own their memory before temporary plaintext is wiped.
          const value = decodeNativeRowValues(
            [{ ...column, name: "value" }],
            new Uint8Array(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength),
          )[0]!;
          decoded[name] = unwrapValue(value, column.column_type, name);
          if (declaration.indexes?.[name]) {
            let indexed = decryptedIndexBytes.get(decoded);
            if (!indexed) decryptedIndexBytes.set(decoded, (indexed = new Map()));
            indexed.set(name, equalityValue(table, name, value));
          }
          pending.delete(name);
        } catch {
          throw new E2eeDataError("invalid-ciphertext");
        } finally {
          plaintext?.fill(0);
        }
      }
    };
    const initial = initialSpaceKeys?.size
      ? initialSpaceKeys.get(`${await db.tableIdentity(scope)}:${identifier}`)
      : undefined;
    if (initial) {
      await decrypt(initial.secret, initial.root);
    } else {
      await withSpaceKeys(db, scope, identifier, decrypt, true);
    }
    if (pending.size) throw new E2eeDataError("invalid-ciphertext");
    result.push(decoded);
  }
  return result;
}
