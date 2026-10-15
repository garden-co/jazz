import { utf8 } from "../../support/bytes.js";
import type {
  ColumnSchema,
  DatabaseSchema,
  EnumSchema,
  IndexSchema,
  ProjectField,
  RecordDescriptor,
  TableSchema,
  TableVariant,
  VariantRecord,
} from "../../_extern/groove-db-storage.js";
import { validateApplicationStorageName } from "../storage/index.js";
import { type Database, type RuntimeRegistryCheckpoint, DbError } from "./index.js";
import { dbTry } from "./commit.js";

const kindOf = (t: any): string => (typeof t === "string" ? t : t.kind);

/** Validate application-selected physical names before any storage mutation. */
export function validateApplicationStorageNames(schema: DatabaseSchema) {
  const seen = new Set<string>();
  for (const name of [
    ...schema.tables.map((t: any) => t.name),
    ...schema.directRecordStores.map((s: any) => s.name),
  ]) {
    try {
      validateApplicationStorageName(name);
    } catch (error) {
      throw new DbError("InvalidApplicationStorageName", {
        name,
        reason: (error as Error).message,
      });
    }
    if (seen.has(name)) throw new DbError("DuplicateApplicationStorageName", name);
    seen.add(name);
  }
}

const TUPLE_MEMBER_KINDS = new Set([
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "Bool",
  "Uuid",
  "EnumTag",
]);
function isSupportedTupleMember(vt: any): boolean {
  const k = kindOf(vt);
  return TUPLE_MEMBER_KINDS.has(k) || (k === "Tuple" && vt.members.every(isSupportedTupleMember));
}
function isSupportedDirectStoreKeyType(vt: any): boolean {
  const k = kindOf(vt);
  return k === "String" || k === "Bytes" || isSupportedTupleMember(vt);
}

export function validateDurableKeySchema(schema: DatabaseSchema) {
  for (const store of schema.directRecordStores)
    if (store.key.some(([, vt]: [string, any]) => !isSupportedDirectStoreKeyType(vt)))
      throw new DbError("InvalidDirectRecordStoreKey", store.name);
  for (const table of schema.tables) validateTableSchemaVariants(table);
}

export function validateTableSchemaVariants(table: TableSchema) {
  if (!table.hasVariants()) return;
  if (table.foreignKeys.length)
    throw new DbError("UnsupportedSchemaVariantTableFeature", table.name);
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw new DbError("MissingPrimaryKey", table.name);
  const versions = new Set<number>();
  for (const variant of table.variants) {
    const fieldError = (field: string) =>
      new DbError("InvalidTableVariantField", { table: table.name, version: variant.tag, field });
    if (variant.tag === 0) throw new DbError("ReservedTableVariant", table.name);
    if (versions.has(variant.tag))
      throw new DbError("DuplicateTableVariant", { table: table.name, version: variant.tag });
    versions.add(variant.tag);
    const fields = new Set<string>();
    if (!variant.payloadFields.length) {
      for (const field of variant.fields) {
        if (fields.has(field) || !table.columns.some((c: any) => c.name === field))
          throw fieldError(field);
        fields.add(field);
      }
    } else {
      const local = new Set<string>();
      for (const field of variant.payloadFields) {
        if (local.has(field.name)) throw fieldError(field.name);
        local.add(field.name);
        const shared = field.sharedColumn;
        if (shared === undefined) continue;
        const valid = table.columns.some(
          (c: any) => c.name === shared && c.columnType.registryCompatibleWith(field.valueType),
        );
        if (!valid || fields.has(shared)) throw fieldError(shared);
        fields.add(shared);
      }
    }
    for (const column of primaryKey.columns)
      if (!fields.has(column.column))
        throw new DbError("SchemaVersionMissingPrimaryKey", {
          table: table.name,
          version: variant.tag,
          column: column.column,
        });
  }
}

const ivm = <T>(f: () => T): T => {
  try {
    return f();
  } catch (error) {
    throw new DbError("IvmRuntime", error);
  }
};

const DECLARED_INDEX_GENERATION_KEY = utf8("\0groove-declared-index-generation");

export class SchemaAdmissionMethods {
  /** Repair schema-declared derived indexes once for a caller-owned generation. */
  async ensureDeclaredIndexGeneration(this: Database, generation: bigint): Promise<void> {
    this.ensureNotPoisoned();
    if (this.residentPublications.size || !this.ivmRuntime.declaredIndexRepairIsIdle())
      throw new DbError("InvalidPersistedIndex", "index repair requires an idle startup runtime");
    const bytes = await dbTry(this.storage.get("indices", DECLARED_INDEX_GENERATION_KEY));
    if (bytes) {
      if (bytes.length !== 8)
        throw new DbError(
          "InvalidPersistedIndex",
          "malformed declared-index generation (expected BE u64)",
        );
      const stored = new DataView(bytes.buffer, bytes.byteOffset).getBigUint64(0);
      if (stored > generation)
        throw new DbError(
          "InvalidPersistedIndex",
          `declared-index generation ${stored} is newer than supported ${generation}`,
        );
      if (stored === generation) return;
    }
    // Poison before the first destructive write; any failure requires reopening.
    this.poisoned = true;
    const registrations = this.ivmRuntime.indexIds().value.pending();
    if (!registrations.isEmpty()) {
      await dbTry(this.storage.writeMany(registrations.operations()));
      this.ivmRuntime.indexIds().value.markPersisted(registrations);
    }
    await dbTry(this.ivmRuntime.rebuildDeclaredIndexes(this.storage));
    await dbTry(this.storage.flushWriteBoundary());
    const value = new Uint8Array(8);
    new DataView(value.buffer).setBigUint64(0, generation);
    await dbTry(this.storage.set("indices", DECLARED_INDEX_GENERATION_KEY, value));
    await dbTry(this.storage.flushWriteBoundary());
    this.poisoned = false;
  }

  /** Capture the process-local registry state before a compound schema activation. */
  runtimeRegistryCheckpoint(this: Database): RuntimeRegistryCheckpoint {
    return {
      ivmRuntime: this.ivmRuntime.clone(),
      storedRecordDescriptors: new Map(
        [...this.storedRecordDescriptors].map(([k, v]) => [k, new Map(v)]),
      ),
    };
  }

  restoreRuntimeRegistry(this: Database, checkpoint: RuntimeRegistryCheckpoint) {
    this.batchPreparationOwner = {};
    this.ivmRuntime = checkpoint.ivmRuntime;
    this.storedRecordDescriptors = checkpoint.storedRecordDescriptors;
  }

  tableSchema(this: Database, table: string): TableSchema {
    return this.table(table);
  }

  /** Append a new table to the live runtime schema. */
  registerTable(this: Database, table: TableSchema) {
    this.batchPreparationOwner = {};
    this.ensureNotPoisoned();
    if (this.ivmRuntime.table(table.name)) throw new DbError("TableAlreadyExists", table.name);
    const updated = this.ivmRuntime.schema().clone();
    updated.tables.push(table);
    validateApplicationStorageNames(updated);
    validateDurableKeySchema(updated);
    ivm(() => this.ivmRuntime.registerTable(table));
  }

  registerTableVariant(this: Database, table: string, variant: TableVariant) {
    this.registerTableVariantWithColumns(table, [], variant);
  }

  /** Append stable catalogue fields and one row layout to a live variant table. */
  registerTableVariantWithColumns(
    this: Database,
    table: string,
    columns: ColumnSchema[],
    variant: TableVariant,
  ) {
    this.batchPreparationOwner = {};
    this.ensureNotPoisoned();
    const updated = this.table(table).clone();
    if (!updated.hasVariants()) throw new DbError("CannotPromoteLiveTableToSchemaVariants", table);
    const added: ColumnSchema[] = [];
    const evolved: ColumnSchema[] = [];
    for (const column of columns) {
      const i = updated.columns.findIndex((c: any) => c.name === column.name);
      if (i < 0) {
        updated.columns.push(column);
        added.push(column);
      } else if (updated.columns[i].equals(column)) {
        // unchanged
      } else if (updated.columns[i].columnType.canEvolveRegistryTo(column.columnType)) {
        updated.columns[i] = column;
        evolved.push(column);
      } else throw new DbError("TableFieldDefinitionMismatch", { table, field: column.name });
    }
    updated.variants.push(variant);
    validateTableSchemaVariants(updated);
    if (evolved.length) ivm(() => this.ivmRuntime.evolveTableVariantRegistries(table, evolved));
    ivm(() => this.ivmRuntime.registerTableVariantWithColumns(table, added, variant));
  }

  /** Append cases to nested enum registries without changing physical identity. */
  evolveTableVariantRegistries(this: Database, table: string, columns: ColumnSchema[]) {
    this.batchPreparationOwner = {};
    this.ensureNotPoisoned();
    const existing = this.table(table);
    for (const desired of columns) {
      const current = existing.columns.find((c: any) => c.name === desired.name);
      if (current && !current.columnType.canEvolveRegistryTo(desired.columnType))
        throw new DbError("TableFieldDefinitionMismatch", { table, field: desired.name });
    }
    ivm(() => this.ivmRuntime.evolveTableVariantRegistries(table, columns));
  }

  /** Append and backfill one durable secondary index on a live table. */
  async registerTableIndex(this: Database, table: string, index: IndexSchema): Promise<void> {
    this.batchPreparationOwner = {};
    this.ensureNotPoisoned();
    const existing = this.table(table);
    const registered = existing.indices.find((r: any) => r.name === index.name);
    if (registered) {
      if (registered.equals(index)) return;
      throw new DbError("TableIndexDefinitionMismatch", { table, index: index.name });
    }
    for (const column of index.columns)
      if (existing.columns.every((c: any) => c.name !== column))
        throw new DbError("TableIndexFieldNotFound", { table, index: index.name, field: column });
    if (this.residentPublications.size)
      throw new DbError("TableIndexRegistrationWhilePublicationsResident", {
        table,
        index: index.name,
      });
    try {
      await this.ivmRuntime.registerTableIndex(table, index, this.storage);
    } catch (error) {
      if ((error as any)?.kind !== "UniqueIndexViolation") this.poisoned = true;
      throw new DbError("IvmRuntime", error);
    }
  }

  defineVariantProjection(this: Database, table: string, target: string, output: RecordDescriptor) {
    this.ensureNotPoisoned();
    ivm(() => this.ivmRuntime.defineVariantProjection(table, target, output));
  }

  registerVariantProjectionCase(
    this: Database,
    table: string,
    target: string,
    tag: number,
    fields: ProjectField[],
  ) {
    this.ensureNotPoisoned();
    ivm(() => this.ivmRuntime.registerVariantProjectionCase(table, target, tag, [...fields]));
  }

  registerVariantProjectionCaseOmittingUnrepresentableEnums(
    this: Database,
    table: string,
    target: string,
    tag: number,
    fields: ProjectField[],
  ) {
    this.ensureNotPoisoned();
    ivm(() =>
      this.ivmRuntime.registerVariantProjectionCaseOmittingUnrepresentableEnums(
        table,
        target,
        tag,
        [...fields],
      ),
    );
  }

  registerVariantCase(
    this: Database,
    table: string,
    target: string,
    tag: number,
    fields: ProjectField[],
  ) {
    this.registerVariantProjectionCase(table, target, tag, fields);
  }

  refreshVariantCaseForRegistryEvolution(
    this: Database,
    table: string,
    target: string,
    tag: number,
    fields: ProjectField[],
  ) {
    this.ensureNotPoisoned();
    ivm(() =>
      this.ivmRuntime.refreshVariantProjectionCaseForRegistryEvolution(table, target, tag, [
        ...fields,
      ]),
    );
  }

  registerVariantProjectionEnumCase(
    this: Database,
    table: string,
    target: string,
    tag: number,
    enumField: string,
    enumSchema: EnumSchema,
    enumCase: string,
    fields: ProjectField[],
  ) {
    this.ensureNotPoisoned();
    ivm(() =>
      this.ivmRuntime.registerVariantProjectionEnumCase(
        table,
        target,
        tag,
        enumField,
        enumSchema,
        enumCase,
        [...fields],
      ),
    );
  }

  registerVariantEnumCase(
    this: Database,
    table: string,
    target: string,
    tag: number,
    enumField: string,
    enumSchema: EnumSchema,
    enumCase: string,
    fields: ProjectField[],
  ) {
    this.registerVariantProjectionEnumCase(
      table,
      target,
      tag,
      enumField,
      enumSchema,
      enumCase,
      fields,
    );
  }

  /** An `Ignore` case emits no rows, distinct from an unregistered case. */
  registerVariantProjectionIgnoreCase(this: Database, table: string, target: string, tag: number) {
    this.ensureNotPoisoned();
    ivm(() => this.ivmRuntime.registerVariantProjectionIgnoreCase(table, target, tag));
  }

  registerVariantIgnoreCase(this: Database, table: string, target: string, tag: number) {
    this.registerVariantProjectionIgnoreCase(table, target, tag);
  }

  projectVariantRecord(this: Database, table: string, target: string, record: VariantRecord) {
    this.ensureNotPoisoned();
    return ivm(() => this.ivmRuntime.projectVariantRecord(table, target, record));
  }
}
