// Stable public schema and session vocabulary.
export * from "./public_api/policy.js";
export * from "./public_api/relation_ir.js";
export * from "./public_api/session.js";
export * from "./public_api/types/index.js";
export { OpenTransactionId, TransactionId } from "./transaction.js";

import * as jsonSchema from "./json_schema.js";
import type { Value } from "./public_api/types/value.js";
import type { ColumnType } from "./public_api/types/schema.js";

/** Validate JSON-bearing public values without changing their source text. */
export function validateJsonValue(value: Value, columnType: ColumnType, path: string): void {
  if (value.type === "Null") return;
  if (value.type === "Text" && columnType.type === "Json") {
    let instance: unknown;
    try {
      instance = JSON.parse(value.value);
    } catch (error) {
      throw new Error(`invalid JSON for column \`${path}\`: ${(error as Error).message}`);
    }
    if (columnType.schema === undefined) return;
    try {
      jsonSchema.validate(columnType.schema, instance);
    } catch (e) {
      if (!(e instanceof jsonSchema.JsonSchemaException)) throw e;
      const { kind, message } = e.error;
      if (kind === "Mismatch")
        throw new Error(`JSON schema validation failed for column \`${path}\`: ${message}`);
      if (kind === "InvalidSchema")
        throw new Error(
          `JSON schema validation failed for column \`${path}\`: invalid declared schema: ${message}`,
        );
      throw new Error(`JSON schema validation unavailable for column \`${path}\`: ${message}`);
    }
    return;
  }
  if (value.type === "Array" && columnType.type === "Array") {
    value.value.forEach((v, i) => validateJsonValue(v, columnType.element, `${path}[${i}]`));
    return;
  }
  if (
    value.type === "Enum" &&
    (columnType.type === "EnumPayload" || columnType.type === "CatalogueEnumPayload")
  ) {
    const selected = columnType.cases.find((c) => c.name === value.case);
    if (!selected) return;
    const n = Math.min(selected.fields.length, value.values.length);
    for (let i = 0; i < n; i++) {
      const f = selected.fields[i];
      validateJsonValue(value.values[i], f.column_type, `${path}.${value.case}.${f.name}`);
    }
  }
}

/** Compile every declared JSON Schema while the public schema is admitted. */
export function validateJsonSchemas(t: ColumnType, path: string): void {
  switch (t.type) {
    case "Json":
      if (t.schema === undefined) return;
      try {
        jsonSchema.checkSchema(t.schema);
      } catch (e) {
        if (!(e instanceof jsonSchema.JsonSchemaException)) throw e;
        if (e.error.kind === "Unavailable")
          throw new Error(
            `JSON schema validation unavailable for column \`${path}\`: ${e.error.message}`,
          );
        throw new Error(`invalid JSON schema for column \`${path}\`: ${e.error.message}`);
      }
      return;
    case "Array":
      return validateJsonSchemas(t.element, `${path}[]`);
    case "Row":
      for (const f of t.columns.columns) validateJsonSchemas(f.column_type, `${path}.${f.name}`);
      return;
    case "EnumPayload":
    case "CatalogueEnumPayload":
      for (const c of t.cases)
        for (const f of c.fields) validateJsonSchemas(f.column_type, `${path}.${c.name}.${f.name}`);
  }
}
