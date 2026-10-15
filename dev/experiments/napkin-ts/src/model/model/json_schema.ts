// JSON Schema validation for JSON columns. The browser build delegates to a
// host-installed validator; compiled validators are cached per schema.

export type JsonSchemaError =
  | { kind: "InvalidSchema"; message: string }
  | { kind: "Mismatch"; message: string }
  | { kind: "Unavailable"; message: string };

export class JsonSchemaException extends Error {
  constructor(readonly error: JsonSchemaError) {
    super(error.message);
  }
}

export interface HostCompiledJsonSchema {
  /** undefined if it matches, a reason if not; throws if the check itself failed. */
  validate(instanceJson: string): string | undefined;
}
export interface HostJsonSchemaValidator {
  compile(schemaJson: string): HostCompiledJsonSchema;
}

let hostValidator: HostJsonSchemaValidator | undefined;
const MAX_COMPILED = 256;
const COMPILED = new Map<string, HostCompiledJsonSchema>();

export function installHostValidator(v: HostJsonSchemaValidator): void {
  hostValidator = v;
  COMPILED.clear();
}

function compiled(schema: unknown): HostCompiledJsonSchema {
  const key = JSON.stringify(schema);
  const hit = COMPILED.get(key);
  if (hit) return hit;
  if (!hostValidator)
    throw new JsonSchemaException({
      kind: "Unavailable",
      message: "no JSON Schema validator is installed in this WASM module",
    });
  let c: HostCompiledJsonSchema;
  try {
    c = hostValidator.compile(key);
  } catch (e) {
    throw new JsonSchemaException({ kind: "InvalidSchema", message: String(e) });
  }
  if (COMPILED.size >= MAX_COMPILED) COMPILED.clear();
  COMPILED.set(key, c);
  return c;
}

export function checkSchema(schema: unknown): void {
  compiled(schema);
}

export function validate(schema: unknown, instance: unknown): void {
  const c = compiled(schema);
  let reason: string | undefined;
  try {
    reason = c.validate(JSON.stringify(instance));
  } catch (e) {
    throw new JsonSchemaException({
      kind: "Unavailable",
      message: `the JSON Schema validator failed: ${e}`,
    });
  }
  if (reason !== undefined) throw new JsonSchemaException({ kind: "Mismatch", message: reason });
}
