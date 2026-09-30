/**
 * The JSON Schema evaluator behind the browser runtime's JSON column checks.
 *
 * Adapted from `@cfworker/json-schema` 4.1.1 (MIT License, Copyright (c)
 * Jeremy Danyow), which evaluates schemas without `eval` and so works under a
 * strict Content-Security-Policy. Changes from upstream, all so verdicts match
 * the native runtime (`jsonschema` 0.42):
 * - numbers may be `bigint` or `IntegralFloat` (see `json-schema-json.ts`) and
 *   compare exactly; draft 4 counts only integer literals as integers;
 * - `multipleOf` is decided like the native runtime rather than with an
 *   epsilon;
 * - `pattern`, `patternProperties` and `format` use the native runtime's regex
 *   syntax and format definitions, with compiled patterns cached;
 * - references resolve against a fixed base URI, not the page's location.
 *
 * It expects schemas that `json-schema-validator.ts` has normalized to the
 * keywords of their dialect.
 */

import { FORMAT_CHECKS } from "./json-schema-formats.js";
import {
  type JsonNumber,
  isJsonInteger,
  isJsonNumber,
  isMultipleOf,
  jsonEqual,
  numericValue,
} from "./json-schema-json.js";
import { compileRegex } from "./json-schema-regex.js";

export type Dialect = "4" | "6" | "7" | "2019-09" | "2020-12";

// Schemas are JSON objects whose keywords this module reads loosely.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Schema = Record<string, any>;

export interface OutputUnit {
  keyword: string;
  keywordLocation: string;
  instanceLocation: string;
  error: string;
}

export interface ValidationResult {
  valid: boolean;
  errors: OutputUnit[];
}

type Lookup = Record<string, Schema | boolean>;
type Evaluated = Record<string | number, boolean>;

interface Context {
  draft: Dialect;
  lookup: Lookup;
  shortCircuit: boolean;
}

const SCHEMA_KEYWORD: Record<string, boolean> = {
  additionalItems: true,
  unevaluatedItems: true,
  items: true,
  contains: true,
  additionalProperties: true,
  unevaluatedProperties: true,
  propertyNames: true,
  not: true,
  if: true,
  // oxlint-disable-next-line unicorn/no-thenable -- a JSON Schema keyword, never awaited
  then: true,
  else: true,
};

const SCHEMA_ARRAY_KEYWORD: Record<string, boolean> = {
  prefixItems: true,
  items: true,
  allOf: true,
  anyOf: true,
  oneOf: true,
};

const SCHEMA_MAP_KEYWORD: Record<string, boolean> = {
  $defs: true,
  definitions: true,
  properties: true,
  patternProperties: true,
  dependentSchemas: true,
};

/** The native runtime's default base URI for schemas without an `$id`. */
const INITIAL_BASE_URI = new URL("json-schema:///");

export function encodePointer(pointer: string): string {
  return encodeURI(pointer.replace(/~/g, "~0").replace(/\//g, "~1"));
}

function isSchemaObject(value: unknown): value is Schema {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Register `schema`, the resources it embeds (`$id`) and their anchors in
 * `lookup` by absolute URI, and resolve every `$ref` against its base URI.
 * Like the native runtime, only subschemas under applicator and definition
 * keywords can declare identifiers; JSON pointers are resolved against the
 * resource document when a reference is followed (see `resolveReference`).
 * The absolute URI of every `$ref` is collected in `references`.
 */
export function dereference(
  schema: Schema | boolean,
  lookup: Lookup = Object.create(null),
  references: string[] = [],
  baseURI: URL = INITIAL_BASE_URI,
  root = true,
): Lookup {
  const register = (uri: string, target: Schema | boolean) => {
    // The first registration wins, like the native registry.
    if (lookup[uri] === undefined) lookup[uri] = target;
  };
  if (typeof schema === "boolean") {
    if (root) register(baseURI.href, schema);
    return lookup;
  }
  if (!isSchemaObject(schema)) return lookup;

  const id = schema.$id ?? schema.id;
  if (typeof id === "string") {
    const url = new URL(id, baseURI.href);
    if (url.hash.length > 1) {
      register(url.href, schema);
    } else {
      url.hash = "";
      baseURI = url;
      register(url.href, schema);
    }
  }
  if (root) register(baseURI.href, schema);

  if (typeof schema.$ref === "string" && schema.__absolute_ref__ === undefined) {
    const url = new URL(schema.$ref, baseURI.href);
    // Normalize the hash (https://url.spec.whatwg.org/#dom-url-hash).
    // eslint-disable-next-line no-self-assign
    url.hash = url.hash;
    Object.defineProperty(schema, "__absolute_ref__", { enumerable: false, value: url.href });
    references.push(url.href);
  }

  if (typeof schema.$recursiveRef === "string" && schema.__absolute_recursive_ref__ === undefined) {
    const url = new URL(schema.$recursiveRef, baseURI.href);
    // eslint-disable-next-line no-self-assign
    url.hash = url.hash;
    Object.defineProperty(schema, "__absolute_recursive_ref__", {
      enumerable: false,
      value: url.href,
    });
  }

  if (typeof schema.$anchor === "string") {
    register(new URL("#" + schema.$anchor, baseURI.href).href, schema);
  }

  for (const key in schema) {
    const subSchema = schema[key];
    if (SCHEMA_ARRAY_KEYWORD[key] && Array.isArray(subSchema)) {
      for (const entry of subSchema) dereference(entry, lookup, references, baseURI, false);
    } else if (SCHEMA_KEYWORD[key]) {
      dereference(subSchema, lookup, references, baseURI, false);
    } else if ((SCHEMA_MAP_KEYWORD[key] || key === "dependencies") && isSchemaObject(subSchema)) {
      for (const subKey in subSchema) {
        dereference(subSchema[subKey], lookup, references, baseURI, false);
      }
    }
  }

  return lookup;
}

/** The schema an absolute URI refers to: a registered one, or a JSON pointer into a resource. */
export function resolveReference(lookup: Lookup, uri: string): Schema | boolean | undefined {
  const registered = lookup[uri];
  if (registered !== undefined) return registered;
  const hash = uri.indexOf("#");
  if (hash < 0) return undefined;
  let node: unknown = lookup[uri.slice(0, hash)];
  const fragment = uri.slice(hash + 1);
  if (node === undefined || !fragment.startsWith("/")) return undefined;
  for (const token of fragment.slice(1).split("/")) {
    let key: string;
    try {
      key = decodeURIComponent(token).replace(/~1/g, "/").replace(/~0/g, "~");
    } catch {
      return undefined;
    }
    if (Array.isArray(node)) {
      if (!/^(?:0|[1-9][0-9]*)$/.test(key)) return undefined;
      node = node[Number(key)];
    } else if (isSchemaObject(node) && Object.prototype.hasOwnProperty.call(node, key)) {
      node = node[key];
    } else {
      return undefined;
    }
  }
  return typeof node === "boolean" || isSchemaObject(node) ? node : undefined;
}

function codePointLength(value: string): number {
  let length = 0;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1);
      if ((next & 0xfc00) === 0xdc00) index++;
    }
    length++;
  }
  return length;
}

function jsonTypeOf(
  instance: unknown,
): "null" | "boolean" | "number" | "string" | "array" | "object" {
  if (instance === null) return "null";
  if (isJsonNumber(instance)) return "number";
  if (Array.isArray(instance)) return "array";
  switch (typeof instance) {
    case "boolean":
    case "string":
      return typeof instance as "boolean" | "string";
    case "object":
      return "object";
    default:
      throw new Error(`Instances of "${typeof instance}" type are not supported.`);
  }
}

/** Check `instance` against a registered schema. */
export function evaluate(
  instance: unknown,
  schema: Schema | boolean,
  draft: Dialect,
  lookup: Lookup,
  shortCircuit: boolean,
): ValidationResult {
  return validate(instance, schema, { draft, lookup, shortCircuit }, null, "#", "#");
}

function validate(
  instance: unknown,
  schema: Schema | boolean,
  context: Context,
  recursiveAnchor: Schema | null,
  instanceLocation: string,
  schemaLocation: string,
  evaluated: Evaluated = Object.create(null),
): ValidationResult {
  if (schema === true) {
    return { valid: true, errors: [] };
  }

  if (schema === false) {
    return {
      valid: false,
      errors: [
        {
          instanceLocation,
          keyword: "false",
          keywordLocation: instanceLocation,
          error: "False boolean schema.",
        },
      ],
    };
  }

  const { draft, lookup, shortCircuit } = context;
  const instanceType = jsonTypeOf(instance);

  const {
    $ref,
    $recursiveRef,
    $recursiveAnchor,
    type: $type,
    const: $const,
    enum: $enum,
    required: $required,
    not: $not,
    anyOf: $anyOf,
    allOf: $allOf,
    oneOf: $oneOf,
    if: $if,
    then: $then,
    else: $else,

    format: $format,

    properties: $properties,
    patternProperties: $patternProperties,
    additionalProperties: $additionalProperties,
    unevaluatedProperties: $unevaluatedProperties,
    minProperties: $minProperties,
    maxProperties: $maxProperties,
    propertyNames: $propertyNames,
    dependentRequired: $dependentRequired,
    dependentSchemas: $dependentSchemas,
    dependencies: $dependencies,

    prefixItems: $prefixItems,
    items: $items,
    additionalItems: $additionalItems,
    unevaluatedItems: $unevaluatedItems,
    contains: $contains,
    minContains: $minContains,
    maxContains: $maxContains,
    minItems: $minItems,
    maxItems: $maxItems,
    uniqueItems: $uniqueItems,

    minimum: $minimum,
    maximum: $maximum,
    exclusiveMinimum: $exclusiveMinimum,
    exclusiveMaximum: $exclusiveMaximum,
    multipleOf: $multipleOf,

    minLength: $minLength,
    maxLength: $maxLength,
    pattern: $pattern,

    __absolute_ref__,
    __absolute_recursive_ref__,
  } = schema;

  const errors: OutputUnit[] = [];

  if ($recursiveAnchor === true && recursiveAnchor === null) {
    recursiveAnchor = schema;
  }

  if ($recursiveRef === "#") {
    const refSchema =
      recursiveAnchor === null
        ? (resolveReference(lookup, __absolute_recursive_ref__) as Schema)
        : recursiveAnchor;
    const keywordLocation = `${schemaLocation}/$recursiveRef`;
    const result = validate(
      instance,
      recursiveAnchor === null ? schema : recursiveAnchor,
      context,
      refSchema,
      instanceLocation,
      keywordLocation,
      evaluated,
    );
    if (!result.valid) {
      errors.push(
        {
          instanceLocation,
          keyword: "$recursiveRef",
          keywordLocation,
          error: "A subschema had errors.",
        },
        ...result.errors,
      );
    }
  }

  if ($ref !== undefined) {
    const refSchema = resolveReference(lookup, __absolute_ref__ ?? $ref);
    if (refSchema === undefined) {
      throw new Error(`Unresolved $ref "${$ref}".`);
    }
    const keywordLocation = `${schemaLocation}/$ref`;
    const result = validate(
      instance,
      refSchema,
      context,
      recursiveAnchor,
      instanceLocation,
      keywordLocation,
      evaluated,
    );
    if (!result.valid) {
      errors.push(
        {
          instanceLocation,
          keyword: "$ref",
          keywordLocation,
          error: "A subschema had errors.",
        },
        ...result.errors,
      );
    }
    if (draft === "4" || draft === "6" || draft === "7") {
      return { valid: errors.length === 0, errors };
    }
  }

  const isInteger =
    instanceType === "number" && isJsonInteger(instance as JsonNumber, draft === "4");

  if (Array.isArray($type)) {
    const valid = $type.some(
      (type: string) => type === instanceType || (type === "integer" && isInteger),
    );
    if (!valid) {
      errors.push({
        instanceLocation,
        keyword: "type",
        keywordLocation: `${schemaLocation}/type`,
        error: `Instance type "${instanceType}" is invalid. Expected "${$type.join('", "')}".`,
      });
    }
  } else if ($type === "integer") {
    if (!isInteger) {
      errors.push({
        instanceLocation,
        keyword: "type",
        keywordLocation: `${schemaLocation}/type`,
        error: `Instance type "${instanceType}" is invalid. Expected "${$type}".`,
      });
    }
  } else if ($type !== undefined && instanceType !== $type) {
    errors.push({
      instanceLocation,
      keyword: "type",
      keywordLocation: `${schemaLocation}/type`,
      error: `Instance type "${instanceType}" is invalid. Expected "${$type}".`,
    });
  }

  if ($const !== undefined && !jsonEqual(instance, $const)) {
    errors.push({
      instanceLocation,
      keyword: "const",
      keywordLocation: `${schemaLocation}/const`,
      error: `Instance does not match ${describeValue($const)}.`,
    });
  }

  if ($enum !== undefined && !$enum.some((value: unknown) => jsonEqual(instance, value))) {
    errors.push({
      instanceLocation,
      keyword: "enum",
      keywordLocation: `${schemaLocation}/enum`,
      error: `Instance does not match any of ${describeValue($enum)}.`,
    });
  }

  if ($not !== undefined) {
    const keywordLocation = `${schemaLocation}/not`;
    const result = validate(
      instance,
      $not,
      context,
      recursiveAnchor,
      instanceLocation,
      keywordLocation,
    );
    if (result.valid) {
      errors.push({
        instanceLocation,
        keyword: "not",
        keywordLocation,
        error: 'Instance matched "not" schema.',
      });
    }
  }

  const subEvaluateds: Evaluated[] = [];

  if ($anyOf !== undefined) {
    const keywordLocation = `${schemaLocation}/anyOf`;
    const errorsLength = errors.length;
    let anyValid = false;
    for (let i = 0; i < $anyOf.length; i++) {
      const subEvaluated: Evaluated = Object.create(evaluated);
      const result = validate(
        instance,
        $anyOf[i],
        context,
        $recursiveAnchor === true ? recursiveAnchor : null,
        instanceLocation,
        `${keywordLocation}/${i}`,
        subEvaluated,
      );
      errors.push(...result.errors);
      anyValid = anyValid || result.valid;
      if (result.valid) {
        subEvaluateds.push(subEvaluated);
      }
    }
    if (anyValid) {
      errors.length = errorsLength;
    } else {
      errors.splice(errorsLength, 0, {
        instanceLocation,
        keyword: "anyOf",
        keywordLocation,
        error: "Instance does not match any subschemas.",
      });
    }
  }

  if ($allOf !== undefined) {
    const keywordLocation = `${schemaLocation}/allOf`;
    const errorsLength = errors.length;
    let allValid = true;
    for (let i = 0; i < $allOf.length; i++) {
      const subEvaluated: Evaluated = Object.create(evaluated);
      const result = validate(
        instance,
        $allOf[i],
        context,
        $recursiveAnchor === true ? recursiveAnchor : null,
        instanceLocation,
        `${keywordLocation}/${i}`,
        subEvaluated,
      );
      errors.push(...result.errors);
      allValid = allValid && result.valid;
      if (result.valid) {
        subEvaluateds.push(subEvaluated);
      }
    }
    if (allValid) {
      errors.length = errorsLength;
    } else {
      errors.splice(errorsLength, 0, {
        instanceLocation,
        keyword: "allOf",
        keywordLocation,
        error: "Instance does not match every subschema.",
      });
    }
  }

  if ($oneOf !== undefined) {
    const keywordLocation = `${schemaLocation}/oneOf`;
    const errorsLength = errors.length;
    const matches = $oneOf.filter((subSchema: Schema | boolean, i: number) => {
      const subEvaluated: Evaluated = Object.create(evaluated);
      const result = validate(
        instance,
        subSchema,
        context,
        $recursiveAnchor === true ? recursiveAnchor : null,
        instanceLocation,
        `${keywordLocation}/${i}`,
        subEvaluated,
      );
      errors.push(...result.errors);
      if (result.valid) {
        subEvaluateds.push(subEvaluated);
      }
      return result.valid;
    }).length;
    if (matches === 1) {
      errors.length = errorsLength;
    } else {
      errors.splice(errorsLength, 0, {
        instanceLocation,
        keyword: "oneOf",
        keywordLocation,
        error: `Instance does not match exactly one subschema (${matches} matches).`,
      });
    }
  }

  if (instanceType === "object" || instanceType === "array") {
    Object.assign(evaluated, ...subEvaluateds);
  }

  if ($if !== undefined) {
    const keywordLocation = `${schemaLocation}/if`;
    // A failing `if` contributes no evaluated items or properties.
    const ifEvaluated: Evaluated = Object.create(evaluated);
    const conditionResult = validate(
      instance,
      $if,
      context,
      recursiveAnchor,
      instanceLocation,
      keywordLocation,
      ifEvaluated,
    ).valid;
    if (conditionResult) {
      Object.assign(evaluated, ifEvaluated);
      if ($then !== undefined) {
        const thenResult = validate(
          instance,
          $then,
          context,
          recursiveAnchor,
          instanceLocation,
          `${schemaLocation}/then`,
          evaluated,
        );
        if (!thenResult.valid) {
          errors.push(
            {
              instanceLocation,
              keyword: "if",
              keywordLocation,
              error: `Instance does not match "then" schema.`,
            },
            ...thenResult.errors,
          );
        }
      }
    } else if ($else !== undefined) {
      const elseResult = validate(
        instance,
        $else,
        context,
        recursiveAnchor,
        instanceLocation,
        `${schemaLocation}/else`,
        evaluated,
      );
      if (!elseResult.valid) {
        errors.push(
          {
            instanceLocation,
            keyword: "if",
            keywordLocation,
            error: `Instance does not match "else" schema.`,
          },
          ...elseResult.errors,
        );
      }
    }
  }

  if (instanceType === "object") {
    const object = instance as Record<string, unknown>;
    if ($required !== undefined) {
      for (const key of $required) {
        if (!(key in object)) {
          errors.push({
            instanceLocation,
            keyword: "required",
            keywordLocation: `${schemaLocation}/required`,
            error: `Instance does not have required property "${key}".`,
          });
        }
      }
    }

    const keys = Object.keys(object);

    if ($minProperties !== undefined && keys.length < $minProperties) {
      errors.push({
        instanceLocation,
        keyword: "minProperties",
        keywordLocation: `${schemaLocation}/minProperties`,
        error: `Instance does not have at least ${$minProperties} properties.`,
      });
    }

    if ($maxProperties !== undefined && keys.length > $maxProperties) {
      errors.push({
        instanceLocation,
        keyword: "maxProperties",
        keywordLocation: `${schemaLocation}/maxProperties`,
        error: `Instance has more than ${$maxProperties} properties.`,
      });
    }

    if ($propertyNames !== undefined) {
      const keywordLocation = `${schemaLocation}/propertyNames`;
      for (const key in object) {
        const subInstancePointer = `${instanceLocation}/${encodePointer(key)}`;
        const result = validate(
          key,
          $propertyNames,
          context,
          recursiveAnchor,
          subInstancePointer,
          keywordLocation,
        );
        if (!result.valid) {
          errors.push(
            {
              instanceLocation,
              keyword: "propertyNames",
              keywordLocation,
              error: `Property name "${key}" does not match schema.`,
            },
            ...result.errors,
          );
        }
      }
    }

    if ($dependentRequired !== undefined) {
      const keywordLocation = `${schemaLocation}/dependentRequired`;
      for (const key in $dependentRequired) {
        if (key in object) {
          for (const dependantKey of $dependentRequired[key] as string[]) {
            if (!(dependantKey in object)) {
              errors.push({
                instanceLocation,
                keyword: "dependentRequired",
                keywordLocation,
                error: `Instance has "${key}" but does not have "${dependantKey}".`,
              });
            }
          }
        }
      }
    }

    if ($dependentSchemas !== undefined) {
      for (const key in $dependentSchemas) {
        const keywordLocation = `${schemaLocation}/dependentSchemas`;
        if (key in object) {
          const result = validate(
            instance,
            $dependentSchemas[key],
            context,
            recursiveAnchor,
            instanceLocation,
            `${keywordLocation}/${encodePointer(key)}`,
            evaluated,
          );
          if (!result.valid) {
            errors.push(
              {
                instanceLocation,
                keyword: "dependentSchemas",
                keywordLocation,
                error: `Instance has "${key}" but does not match dependant schema.`,
              },
              ...result.errors,
            );
          }
        }
      }
    }

    if ($dependencies !== undefined) {
      const keywordLocation = `${schemaLocation}/dependencies`;
      for (const key in $dependencies) {
        if (key in object) {
          const propsOrSchema = $dependencies[key] as Schema | string[];
          if (Array.isArray(propsOrSchema)) {
            for (const dependantKey of propsOrSchema) {
              if (!(dependantKey in object)) {
                errors.push({
                  instanceLocation,
                  keyword: "dependencies",
                  keywordLocation,
                  error: `Instance has "${key}" but does not have "${dependantKey}".`,
                });
              }
            }
          } else {
            const result = validate(
              instance,
              propsOrSchema,
              context,
              recursiveAnchor,
              instanceLocation,
              `${keywordLocation}/${encodePointer(key)}`,
            );
            if (!result.valid) {
              errors.push(
                {
                  instanceLocation,
                  keyword: "dependencies",
                  keywordLocation,
                  error: `Instance has "${key}" but does not match dependant schema.`,
                },
                ...result.errors,
              );
            }
          }
        }
      }
    }

    const thisEvaluated: Evaluated = Object.create(null);

    let stop = false;

    if ($properties !== undefined) {
      const keywordLocation = `${schemaLocation}/properties`;
      for (const key in $properties) {
        if (!(key in object)) {
          continue;
        }
        const subInstancePointer = `${instanceLocation}/${encodePointer(key)}`;
        const result = validate(
          object[key],
          $properties[key],
          context,
          recursiveAnchor,
          subInstancePointer,
          `${keywordLocation}/${encodePointer(key)}`,
        );
        if (result.valid) {
          evaluated[key] = thisEvaluated[key] = true;
        } else {
          stop = shortCircuit;
          errors.push(
            {
              instanceLocation,
              keyword: "properties",
              keywordLocation,
              error: `Property "${key}" does not match schema.`,
            },
            ...result.errors,
          );
          if (stop) break;
        }
      }
    }

    if (!stop && $patternProperties !== undefined) {
      const keywordLocation = `${schemaLocation}/patternProperties`;
      for (const pattern in $patternProperties) {
        const regex = compileRegex(pattern);
        const subSchema = $patternProperties[pattern];
        for (const key in object) {
          if (!regex.test(key)) {
            continue;
          }
          const subInstancePointer = `${instanceLocation}/${encodePointer(key)}`;
          const result = validate(
            object[key],
            subSchema,
            context,
            recursiveAnchor,
            subInstancePointer,
            `${keywordLocation}/${encodePointer(pattern)}`,
          );
          if (result.valid) {
            evaluated[key] = thisEvaluated[key] = true;
          } else {
            stop = shortCircuit;
            errors.push(
              {
                instanceLocation,
                keyword: "patternProperties",
                keywordLocation,
                error: `Property "${key}" matches pattern "${pattern}" but does not match associated schema.`,
              },
              ...result.errors,
            );
          }
        }
      }
    }

    if (!stop && $additionalProperties !== undefined) {
      const keywordLocation = `${schemaLocation}/additionalProperties`;
      for (const key in object) {
        if (thisEvaluated[key]) {
          continue;
        }
        const subInstancePointer = `${instanceLocation}/${encodePointer(key)}`;
        const result = validate(
          object[key],
          $additionalProperties,
          context,
          recursiveAnchor,
          subInstancePointer,
          keywordLocation,
        );
        if (result.valid) {
          evaluated[key] = true;
        } else {
          stop = shortCircuit;
          errors.push(
            {
              instanceLocation,
              keyword: "additionalProperties",
              keywordLocation,
              error: `Property "${key}" does not match additional properties schema.`,
            },
            ...result.errors,
          );
        }
      }
    } else if (!stop && $unevaluatedProperties !== undefined) {
      const keywordLocation = `${schemaLocation}/unevaluatedProperties`;
      for (const key in object) {
        if (!evaluated[key]) {
          const subInstancePointer = `${instanceLocation}/${encodePointer(key)}`;
          const result = validate(
            object[key],
            $unevaluatedProperties,
            context,
            recursiveAnchor,
            subInstancePointer,
            keywordLocation,
          );
          if (result.valid) {
            evaluated[key] = true;
          } else {
            errors.push(
              {
                instanceLocation,
                keyword: "unevaluatedProperties",
                keywordLocation,
                error: `Property "${key}" does not match unevaluated properties schema.`,
              },
              ...result.errors,
            );
          }
        }
      }
    }
  } else if (instanceType === "array") {
    const array = instance as unknown[];
    const length = array.length;

    if ($maxItems !== undefined && length > $maxItems) {
      errors.push({
        instanceLocation,
        keyword: "maxItems",
        keywordLocation: `${schemaLocation}/maxItems`,
        error: `Array has too many items (${length} > ${$maxItems}).`,
      });
    }

    if ($minItems !== undefined && length < $minItems) {
      errors.push({
        instanceLocation,
        keyword: "minItems",
        keywordLocation: `${schemaLocation}/minItems`,
        error: `Array has too few items (${length} < ${$minItems}).`,
      });
    }

    let i = 0;
    let stop = false;

    if ($prefixItems !== undefined) {
      const keywordLocation = `${schemaLocation}/prefixItems`;
      const length2 = Math.min($prefixItems.length, length);
      for (; i < length2; i++) {
        const result = validate(
          array[i],
          $prefixItems[i],
          context,
          recursiveAnchor,
          `${instanceLocation}/${i}`,
          `${keywordLocation}/${i}`,
        );
        evaluated[i] = true;
        if (!result.valid) {
          stop = shortCircuit;
          errors.push(
            {
              instanceLocation,
              keyword: "prefixItems",
              keywordLocation,
              error: `Items did not match schema.`,
            },
            ...result.errors,
          );
          if (stop) break;
        }
      }
    }

    if ($items !== undefined) {
      const keywordLocation = `${schemaLocation}/items`;
      if (Array.isArray($items)) {
        const length2 = Math.min($items.length, length);
        for (; i < length2; i++) {
          const result = validate(
            array[i],
            $items[i],
            context,
            recursiveAnchor,
            `${instanceLocation}/${i}`,
            `${keywordLocation}/${i}`,
          );
          evaluated[i] = true;
          if (!result.valid) {
            stop = shortCircuit;
            errors.push(
              {
                instanceLocation,
                keyword: "items",
                keywordLocation,
                error: `Items did not match schema.`,
              },
              ...result.errors,
            );
            if (stop) break;
          }
        }
      } else {
        for (; i < length; i++) {
          const result = validate(
            array[i],
            $items,
            context,
            recursiveAnchor,
            `${instanceLocation}/${i}`,
            keywordLocation,
          );
          evaluated[i] = true;
          if (!result.valid) {
            stop = shortCircuit;
            errors.push(
              {
                instanceLocation,
                keyword: "items",
                keywordLocation,
                error: `Items did not match schema.`,
              },
              ...result.errors,
            );
            if (stop) break;
          }
        }
      }

      if (!stop && $additionalItems !== undefined) {
        const keywordLocation = `${schemaLocation}/additionalItems`;
        for (; i < length; i++) {
          const result = validate(
            array[i],
            $additionalItems,
            context,
            recursiveAnchor,
            `${instanceLocation}/${i}`,
            keywordLocation,
          );
          evaluated[i] = true;
          if (!result.valid) {
            stop = shortCircuit;
            errors.push(
              {
                instanceLocation,
                keyword: "additionalItems",
                keywordLocation,
                error: `Items did not match additional items schema.`,
              },
              ...result.errors,
            );
          }
        }
      }
    }

    if ($contains !== undefined) {
      if (length === 0 && $minContains === undefined) {
        errors.push({
          instanceLocation,
          keyword: "contains",
          keywordLocation: `${schemaLocation}/contains`,
          error: `Array is empty. It must contain at least one item matching the schema.`,
        });
      } else if ($minContains !== undefined && length < $minContains) {
        errors.push({
          instanceLocation,
          keyword: "minContains",
          keywordLocation: `${schemaLocation}/minContains`,
          error: `Array has less items (${length}) than minContains (${$minContains}).`,
        });
      } else {
        const keywordLocation = `${schemaLocation}/contains`;
        const errorsLength = errors.length;
        let contained = 0;
        for (let j = 0; j < length; j++) {
          const result = validate(
            array[j],
            $contains,
            context,
            recursiveAnchor,
            `${instanceLocation}/${j}`,
            keywordLocation,
          );
          if (result.valid) {
            evaluated[j] = true;
            contained++;
          } else {
            errors.push(...result.errors);
          }
        }

        if (contained >= ($minContains || 0)) {
          errors.length = errorsLength;
        }

        if ($minContains === undefined && $maxContains === undefined && contained === 0) {
          errors.splice(errorsLength, 0, {
            instanceLocation,
            keyword: "contains",
            keywordLocation,
            error: `Array does not contain item matching schema.`,
          });
        } else if ($minContains !== undefined && contained < $minContains) {
          errors.push({
            instanceLocation,
            keyword: "minContains",
            keywordLocation: `${schemaLocation}/minContains`,
            error: `Array must contain at least ${$minContains} items matching schema. Only ${contained} items were found.`,
          });
        } else if ($maxContains !== undefined && contained > $maxContains) {
          errors.push({
            instanceLocation,
            keyword: "maxContains",
            keywordLocation: `${schemaLocation}/maxContains`,
            error: `Array may contain at most ${$maxContains} items matching schema. ${contained} items were found.`,
          });
        }
      }
    }

    if (!stop && $unevaluatedItems !== undefined) {
      const keywordLocation = `${schemaLocation}/unevaluatedItems`;
      for (; i < length; i++) {
        if (evaluated[i]) {
          continue;
        }
        const result = validate(
          array[i],
          $unevaluatedItems,
          context,
          recursiveAnchor,
          `${instanceLocation}/${i}`,
          keywordLocation,
        );
        evaluated[i] = true;
        if (!result.valid) {
          errors.push(
            {
              instanceLocation,
              keyword: "unevaluatedItems",
              keywordLocation,
              error: `Items did not match unevaluated items schema.`,
            },
            ...result.errors,
          );
        }
      }
    }

    if ($uniqueItems) {
      outer: for (let j = 0; j < length; j++) {
        for (let k = j + 1; k < length; k++) {
          if (jsonEqual(array[j], array[k])) {
            errors.push({
              instanceLocation,
              keyword: "uniqueItems",
              keywordLocation: `${schemaLocation}/uniqueItems`,
              error: `Duplicate items at indexes ${j} and ${k}.`,
            });
            break outer;
          }
        }
      }
    }
  } else if (instanceType === "number") {
    const value = numericValue(instance as JsonNumber);
    if (draft === "4") {
      if (
        $minimum !== undefined &&
        (($exclusiveMinimum === true && value <= $minimum) || value < $minimum)
      ) {
        errors.push({
          instanceLocation,
          keyword: "minimum",
          keywordLocation: `${schemaLocation}/minimum`,
          error: `${value} is less than ${$exclusiveMinimum ? "or equal to " : ""}${$minimum}.`,
        });
      }
      if (
        $maximum !== undefined &&
        (($exclusiveMaximum === true && value >= $maximum) || value > $maximum)
      ) {
        errors.push({
          instanceLocation,
          keyword: "maximum",
          keywordLocation: `${schemaLocation}/maximum`,
          error: `${value} is greater than ${$exclusiveMaximum ? "or equal to " : ""}${$maximum}.`,
        });
      }
    } else {
      if ($minimum !== undefined && value < $minimum) {
        errors.push({
          instanceLocation,
          keyword: "minimum",
          keywordLocation: `${schemaLocation}/minimum`,
          error: `${value} is less than ${$minimum}.`,
        });
      }
      if ($maximum !== undefined && value > $maximum) {
        errors.push({
          instanceLocation,
          keyword: "maximum",
          keywordLocation: `${schemaLocation}/maximum`,
          error: `${value} is greater than ${$maximum}.`,
        });
      }
      if ($exclusiveMinimum !== undefined && value <= $exclusiveMinimum) {
        errors.push({
          instanceLocation,
          keyword: "exclusiveMinimum",
          keywordLocation: `${schemaLocation}/exclusiveMinimum`,
          error: `${value} is less than or equal to ${$exclusiveMinimum}.`,
        });
      }
      if ($exclusiveMaximum !== undefined && value >= $exclusiveMaximum) {
        errors.push({
          instanceLocation,
          keyword: "exclusiveMaximum",
          keywordLocation: `${schemaLocation}/exclusiveMaximum`,
          error: `${value} is greater than or equal to ${$exclusiveMaximum}.`,
        });
      }
    }
    if ($multipleOf !== undefined && !isMultipleOf(instance as JsonNumber, $multipleOf)) {
      errors.push({
        instanceLocation,
        keyword: "multipleOf",
        keywordLocation: `${schemaLocation}/multipleOf`,
        error: `${value} is not a multiple of ${$multipleOf}.`,
      });
    }
  } else if (instanceType === "string") {
    const string = instance as string;
    const length =
      $minLength === undefined && $maxLength === undefined ? 0 : codePointLength(string);
    if ($minLength !== undefined && length < $minLength) {
      errors.push({
        instanceLocation,
        keyword: "minLength",
        keywordLocation: `${schemaLocation}/minLength`,
        error: `String is too short (${length} < ${$minLength}).`,
      });
    }
    if ($maxLength !== undefined && length > $maxLength) {
      errors.push({
        instanceLocation,
        keyword: "maxLength",
        keywordLocation: `${schemaLocation}/maxLength`,
        error: `String is too long (${length} > ${$maxLength}).`,
      });
    }
    if ($pattern !== undefined && !compileRegex($pattern).test(string)) {
      errors.push({
        instanceLocation,
        keyword: "pattern",
        keywordLocation: `${schemaLocation}/pattern`,
        error: `String does not match pattern.`,
      });
    }
    const check = $format === undefined ? undefined : FORMAT_CHECKS[$format];
    if (check && !check(string)) {
      errors.push({
        instanceLocation,
        keyword: "format",
        keywordLocation: `${schemaLocation}/format`,
        error: `String does not match format "${$format}".`,
      });
    }
  }

  return { valid: errors.length === 0, errors };
}

function describeValue(value: unknown): string {
  return JSON.stringify(value, (_key, entry) =>
    typeof entry === "bigint" ? Number(entry) : isJsonNumber(entry) ? numericValue(entry) : entry,
  );
}
