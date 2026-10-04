import {
  type Dialect,
  type OutputUnit,
  type Schema,
  dereference,
  evaluate,
  resolveReference,
} from "./json-schema-evaluate.js";
import { parseJson, withPlainNumbers } from "./json-schema-json.js";
import { META_SCHEMAS } from "./json-schema-metaschemas.js";
import { RegexSyntaxError, UnsupportedRegexError, compileRegex } from "./json-schema-regex.js";

/**
 * JSON Schema validation for the browser WASM runtime.
 *
 * Native runtimes validate JSON columns with the Rust `jsonschema` crate. The
 * browser build leaves that crate out (it and its regex/IDNA/bignum subtree
 * are a large share of the WASM binary) and calls this module instead, through
 * the callback `installJsonSchemaValidator` hands to the WASM module.
 *
 * The evaluator underneath (`json-schema-evaluate.ts`) needs no `eval`, so it
 * works under a strict Content-Security-Policy. This module gives it the same
 * contract as `jsonschema::validator_for` + `Validator::validate`:
 * - the dialect comes from `$schema`, defaulting to 2020-12;
 * - a declared schema that fails its dialect's meta-schema, has an invalid
 *   regular expression, or has a `$ref` that does not resolve locally is
 *   rejected when it is compiled, not when a value is written;
 * - keywords a dialect does not define are ignored, as are the siblings of
 *   `$ref` in drafts 4, 6 and 7;
 * - `format` is asserted in drafts 4, 6 and 7 (for the formats the native
 *   runtime knows in that draft, with its definitions) and only annotates in
 *   2019-09 and 2020-12;
 * - `contentEncoding` is asserted in drafts 6 and 7;
 * - patterns use the native regex syntax, numbers compare exactly (including
 *   integers beyond 2^53), and `multipleOf` is decided as natively.
 *
 * Features it cannot express are rejected explicitly instead of being skipped,
 * so the browser never accepts a value a native runtime would refuse on those
 * grounds: `$dynamicRef`/`$dynamicAnchor`, draft 6/7 `contentMediaType`, and
 * the internationalized formats (`idn-email`, `idn-hostname`, `iri`,
 * `iri-reference`) in the drafts that assert formats, and the few regex
 * constructs `json-schema-regex.ts` lists.
 */

export type JsonSchemaCheck = (instanceJson: string) => string | undefined;

const DIALECT_BY_URI: Record<string, Dialect> = {
  "json-schema.org/draft/2020-12/schema": "2020-12",
  "json-schema.org/draft/2019-09/schema": "2019-09",
  "json-schema.org/draft-07/schema": "7",
  "json-schema.org/draft-06/schema": "6",
  "json-schema.org/draft-04/schema": "4",
};

/**
 * Every keyword the evaluator acts on, whatever the draft. The ones
 * a dialect does not define are removed before it sees the schema; all other
 * unknown keywords are kept, so `$ref`s into them still resolve.
 */
const VALIDATOR_KEYWORDS = new Set([
  "$anchor",
  "$id",
  "$recursiveAnchor",
  "$recursiveRef",
  "$ref",
  "additionalItems",
  "additionalProperties",
  "allOf",
  "anyOf",
  "const",
  "contains",
  "dependencies",
  "dependentRequired",
  "dependentSchemas",
  "else",
  "enum",
  "exclusiveMaximum",
  "exclusiveMinimum",
  "format",
  "id",
  "if",
  "items",
  "maxContains",
  "maxItems",
  "maxLength",
  "maxProperties",
  "maximum",
  "minContains",
  "minItems",
  "minLength",
  "minProperties",
  "minimum",
  "multipleOf",
  "not",
  "oneOf",
  "pattern",
  "patternProperties",
  "prefixItems",
  "properties",
  "propertyNames",
  "required",
  "then",
  "type",
  "unevaluatedItems",
  "unevaluatedProperties",
  "uniqueItems",
]);

interface Vocabulary {
  /** Keywords holding one subschema. */
  single: string[];
  /** Keywords holding an array of subschemas. */
  array: string[];
  /** Keywords holding a map of subschemas. */
  map: string[];
  /** Keywords holding plain values. */
  plain: string[];
}

const DRAFT4: Vocabulary = {
  single: ["additionalItems", "additionalProperties", "items", "not"],
  array: ["allOf", "anyOf", "items", "oneOf"],
  map: ["definitions", "dependencies", "patternProperties", "properties"],
  plain: [
    "$ref",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "format",
    "id",
    "maxItems",
    "maxLength",
    "maxProperties",
    "maximum",
    "minItems",
    "minLength",
    "minProperties",
    "minimum",
    "multipleOf",
    "pattern",
    "required",
    "type",
    "uniqueItems",
  ],
};

const DRAFT6: Vocabulary = {
  ...DRAFT4,
  single: [...DRAFT4.single, "contains", "propertyNames"],
  plain: [...DRAFT4.plain.filter((keyword) => keyword !== "id"), "$id", "const"],
};

const DRAFT7: Vocabulary = { ...DRAFT6, single: [...DRAFT6.single, "if", "then", "else"] };

const DRAFT2019: Vocabulary = {
  single: [...DRAFT7.single, "unevaluatedItems", "unevaluatedProperties"],
  array: DRAFT7.array,
  map: [...DRAFT7.map, "$defs", "dependentSchemas"],
  plain: [
    // `format` only annotates from 2019-09 on.
    ...DRAFT7.plain.filter((keyword) => keyword !== "format"),
    "$anchor",
    "$recursiveAnchor",
    "$recursiveRef",
    "dependentRequired",
    "maxContains",
    "minContains",
  ],
};

const DRAFT2020: Vocabulary = {
  single: DRAFT2019.single.filter((keyword) => keyword !== "additionalItems"),
  array: ["allOf", "anyOf", "oneOf", "prefixItems"],
  map: DRAFT2019.map,
  plain: DRAFT2019.plain.filter(
    (keyword) => keyword !== "$recursiveAnchor" && keyword !== "$recursiveRef",
  ),
};

const VOCABULARIES: Record<Dialect, Vocabulary> = {
  "4": DRAFT4,
  "6": DRAFT6,
  "7": DRAFT7,
  "2019-09": DRAFT2019,
  "2020-12": DRAFT2020,
};

/** Keywords a dialect defines that the evaluator cannot evaluate. */
const UNSUPPORTED_KEYWORDS: Record<Dialect, string[]> = {
  "4": [],
  "6": ["contentMediaType"],
  "7": ["contentMediaType"],
  "2019-09": [],
  "2020-12": ["$dynamicAnchor", "$dynamicRef"],
};

/**
 * `format`s the native runtime asserts, per legacy draft (later drafts only
 * annotate). Others are ignored there, so they are dropped here too.
 */
const DRAFT4_FORMATS = [
  "date",
  "date-time",
  "email",
  "hostname",
  "idn-email",
  "ipv4",
  "ipv6",
  "regex",
  "time",
  "uri",
];
const DRAFT6_FORMATS = [...DRAFT4_FORMATS, "json-pointer", "uri-reference", "uri-template"];
const ASSERTED_FORMATS: Partial<Record<Dialect, string[]>> = {
  "4": DRAFT4_FORMATS,
  "6": DRAFT6_FORMATS,
  "7": [...DRAFT6_FORMATS, "idn-hostname", "iri", "iri-reference", "relative-json-pointer"],
};

/** Asserted `format`s the browser has no check for (they need IDNA tables). */
const UNSUPPORTED_FORMATS = ["idn-email", "idn-hostname", "iri", "iri-reference"];

/**
 * `contentEncoding` values the native runtime checks in drafts 6 and 7, as
 * patterns matching exactly the strings its RFC 4648 decoders accept: padded,
 * with unused trailing bits zero.
 */
const CONTENT_ENCODING_PATTERNS: Record<string, string> = {
  base64: base64Pattern("A-Za-z0-9+/"),
  base64url: base64Pattern("A-Za-z0-9\\-_"),
  base32: base32Pattern("A-Z2-7", "AEIMQUY4", "AQ", "ACEGIKMOQSUWY246", "AIQY"),
  base32hex: base32Pattern("0-9A-V", "048CGKOS", "0G", "02468ACEGIKMOQSU", "08GO"),
  base16: "^(?:[0-9A-Fa-f]{2})*$",
};

function base64Pattern(alphabet: string): string {
  const char = `[${alphabet}]`;
  return `^(?:${char}{4})*(?:${char}[AQgw]==|${char}{2}[AEIMQUYcgkosw048]=)?$`;
}

/** Final 2, 4, 5 and 7 character groups each constrain their last character. */
function base32Pattern(
  alphabet: string,
  last2: string,
  last4: string,
  last5: string,
  last7: string,
): string {
  const char = `[${alphabet}]`;
  return (
    `^(?:${char}{8})*(?:${char}[${last2}]={6}|${char}{3}[${last4}]={4}` +
    `|${char}{4}[${last5}]={3}|${char}{6}[${last7}]=)?$`
  );
}

class InvalidSchemaError extends Error {}

function dialectOf(schema: unknown): Dialect {
  if (!isObject(schema)) return "2020-12";
  const declared = schema.$schema;
  if (typeof declared !== "string") return "2020-12";
  const uri = declared.replace(/#+$/, "").replace(/^https?:\/\//, "");
  const dialect = DIALECT_BY_URI[uri];
  if (!dialect) {
    throw new InvalidSchemaError(`unknown JSON Schema dialect "${declared}"`);
  }
  return dialect;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Copy `schema` so the evaluator follows `dialect`: it evaluates
 * every keyword it knows regardless of draft, so the ones `dialect` does not
 * define are dropped, and draft 6/7 content encodings become patterns.
 * `allowed` admits otherwise unsupported keywords (for the meta-schemas).
 */
function normalizeSchema(
  schema: unknown,
  dialect: Dialect,
  allowed: readonly string[] = [],
): unknown {
  if (typeof schema === "boolean") return schema;
  if (!isObject(schema)) return {};
  const vocabulary = VOCABULARIES[dialect];
  const defined = new Set([
    ...vocabulary.single,
    ...vocabulary.array,
    ...vocabulary.map,
    ...vocabulary.plain,
    ...allowed,
  ]);
  // Drafts 4-7 ignore every keyword next to `$ref`.
  const refOnly = typeof schema.$ref === "string" && ["4", "6", "7"].includes(dialect);
  const entries: [string, unknown][] = [];
  for (const [keyword, value] of Object.entries(schema)) {
    if (UNSUPPORTED_KEYWORDS[dialect].includes(keyword) && !allowed.includes(keyword)) {
      throw new InvalidSchemaError(
        `the \`${keyword}\` keyword is not supported by the browser runtime yet`,
      );
    }
    if (VALIDATOR_KEYWORDS.has(keyword)) {
      if (!defined.has(keyword) || (refOnly && keyword !== "$ref")) continue;
    }
    // Patterns are compiled when the schema is, as natively.
    if (keyword === "pattern" && typeof value === "string") compilePattern(value);
    if (keyword === "patternProperties" && isObject(value)) {
      for (const pattern of Object.keys(value)) compilePattern(pattern);
    }
    if (keyword === "format" && typeof value === "string") {
      if (!ASSERTED_FORMATS[dialect]?.includes(value)) continue;
      if (UNSUPPORTED_FORMATS.includes(value)) {
        throw new InvalidSchemaError(
          `the "${value}" format is not supported by the browser runtime yet`,
        );
      }
    }
    if (vocabulary.array.includes(keyword) && Array.isArray(value)) {
      entries.push([keyword, value.map((entry) => normalizeSchema(entry, dialect, allowed))]);
    } else if (vocabulary.single.includes(keyword)) {
      entries.push([keyword, normalizeSchema(value, dialect, allowed)]);
    } else if (vocabulary.map.includes(keyword) && isObject(value)) {
      entries.push([keyword, normalizeMap(value, dialect, allowed)]);
    } else {
      entries.push([keyword, value]);
    }
  }
  const encoding = schema.contentEncoding;
  if (!refOnly && (dialect === "6" || dialect === "7") && typeof encoding === "string") {
    const pattern = CONTENT_ENCODING_PATTERNS[encoding];
    if (pattern) {
      const allOf = entries.find(([keyword]) => keyword === "allOf");
      if (allOf) allOf[1] = [...(allOf[1] as unknown[]), { pattern }];
      else entries.push(["allOf", [{ pattern }]]);
    }
  }
  // `fromEntries` defines own properties, so a `__proto__` key stays a key.
  return Object.fromEntries(entries);
}

function normalizeMap(
  value: Record<string, unknown>,
  dialect: Dialect,
  allowed: readonly string[],
): unknown {
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      // `dependencies` values may be property-name arrays rather than schemas.
      Array.isArray(entry) ? entry : normalizeSchema(entry, dialect, allowed),
    ]),
  );
}

function compilePattern(pattern: string): void {
  try {
    compileRegex(pattern);
  } catch (error) {
    if (error instanceof RegexSyntaxError) {
      throw new InvalidSchemaError(
        `"${pattern}" is not a valid regular expression: ${error.message}`,
      );
    }
    if (error instanceof UnsupportedRegexError) {
      throw new InvalidSchemaError(
        `the regular expression "${pattern}" is not supported by the browser runtime yet: ${error.message}`,
      );
    }
    throw error;
  }
}

/** Reject references that resolve neither inside the schema nor to a meta-schema. */
function checkReferences(validator: CompiledSchema): void {
  for (const reference of validator.references) {
    if (resolveReference(validator.lookup, reference) === undefined) {
      throw new InvalidSchemaError(
        `unresolvable reference "${decodeURI(reference.replace(/^json-schema:\/\/\//, ""))}"`,
      );
    }
  }
}

/**
 * The 2020-12 meta-schema recurses through `$dynamicRef: "#meta"`. Without
 * dialect extensions that always resolves to the 2020-12 root, which is what
 * 2019-09's `$recursiveRef: "#"` expresses and the evaluator understands.
 */
function withRecursiveRefs(schema: unknown): unknown {
  if (Array.isArray(schema)) return schema.map(withRecursiveRefs);
  if (!isObject(schema)) return schema;
  const entries: [string, unknown][] = [];
  for (const [keyword, value] of Object.entries(schema)) {
    if (keyword === "$dynamicAnchor") entries.push(["$recursiveAnchor", true]);
    else if (keyword === "$dynamicRef") entries.push(["$recursiveRef", "#"]);
    else entries.push([keyword, withRecursiveRefs(value)]);
  }
  return Object.fromEntries(entries);
}

const META_SCHEMA_SOURCES: Record<Dialect, readonly unknown[]> = {
  "4": META_SCHEMAS.draft4,
  "6": META_SCHEMAS.draft6,
  "7": META_SCHEMAS.draft7,
  "2019-09": META_SCHEMAS.draft201909,
  "2020-12": META_SCHEMAS.draft202012,
};

/**
 * Fresh copies of a dialect's meta-schemas (root first), normalized under the
 * dialect's own rules. Registering annotates the schemas, so every compiled
 * schema gets its own copies.
 */
function metaSchemas(dialect: Dialect): Schema[] {
  return META_SCHEMA_SOURCES[dialect].map((schema) =>
    dialect === "2020-12"
      ? (withRecursiveRefs(
          normalizeSchema(schema, dialect, ["$dynamicAnchor", "$dynamicRef"]),
        ) as Schema)
      : (normalizeSchema(schema, dialect) as Schema),
  );
}

/** A schema registered with the meta-schemas of its dialect, ready to evaluate. */
class CompiledSchema {
  readonly lookup: Record<string, Schema | boolean>;
  readonly references: string[] = [];

  constructor(
    private readonly schema: Schema | boolean,
    private readonly dialect: Dialect,
    private readonly shortCircuit: boolean,
    additional: Schema[],
  ) {
    this.lookup = dereference(schema, undefined, this.references);
    for (const extra of additional) dereference(extra, this.lookup);
  }

  validate(instance: unknown) {
    return evaluate(instance, this.schema, this.dialect, this.lookup, this.shortCircuit);
  }
}

const metaValidators = new Map<Dialect, CompiledSchema>();

function metaValidator(dialect: Dialect): CompiledSchema {
  let validator = metaValidators.get(dialect);
  if (!validator) {
    const [root, ...vocabularies] = metaSchemas(dialect);
    validator = new CompiledSchema(root!, dialect, false, vocabularies);
    metaValidators.set(dialect, validator);
  }
  return validator;
}

function describe(errors: OutputUnit[]): string {
  // Errors are listed outermost first; the last one is the most specific.
  const error = errors[errors.length - 1];
  if (!error) return "value does not match the schema";
  const location = error.instanceLocation.replace(/^#/, "");
  return location ? `${error.error} (at ${location})` : error.error;
}

/**
 * Compile a declared JSON Schema. Throws when the schema itself is invalid;
 * the returned check says why an instance does not match, or `undefined`.
 */
export function compileJsonSchema(schema: unknown): JsonSchemaCheck {
  const dialect = dialectOf(schema);
  if (typeof schema === "boolean") {
    if (dialect === "4") throw new InvalidSchemaError("boolean schemas need draft 6 or later");
  } else {
    const meta = metaValidator(dialect).validate(schema);
    if (!meta.valid) throw new InvalidSchemaError(describe(meta.errors));
  }
  const normalized = normalizeSchema(withPlainNumbers(schema), dialect) as Schema | boolean;
  const validator = new CompiledSchema(normalized, dialect, true, metaSchemas(dialect));
  checkReferences(validator);
  return (instanceJson) => {
    const result = validator.validate(parseJson(instanceJson));
    return result.valid ? undefined : describe(result.errors);
  };
}

/**
 * The callback the browser WASM module calls: compile a declared schema given
 * as JSON text, or throw an `Error` saying why it is invalid.
 */
export function compileJsonSchemaText(schemaJson: string): JsonSchemaCheck {
  return compileJsonSchema(parseJson(schemaJson));
}

/** Make `wasmModule` validate JSON columns with this module. */
export function installJsonSchemaValidator(wasmModule: {
  setJsonSchemaValidator(compile: (schemaJson: string) => JsonSchemaCheck): void;
}): void {
  wasmModule.setJsonSchemaValidator(compileJsonSchemaText);
}
