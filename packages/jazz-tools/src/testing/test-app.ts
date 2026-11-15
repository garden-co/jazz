/**
 * Apps defined inline, for tests: a `jazz.app.json` (and its
 * `jazz.permissions.json`) written as an object in the test, compiled by
 * Jazz's Rust loader exactly as `jazz-tools generate` compiles the files
 * into `jazz.generated.ts`. Node only: it loads jazz-napi synchronously, so
 * a test can define its app at the top level.
 *
 * ```ts
 * const app = testApp({
 *   tables: { todos: { columns: { title: "string", done: "boolean" } } },
 * });
 * const permissions = { tables: { todos: { read: "true", insert: "true" } } };
 * ```
 *
 * The app's types are inferred from the object ({@link InferAppTypes}), as
 * `jazz-tools generate` would write them into `jazz.generated.ts`: rows,
 * inserts, columns, relations, computed fields and transforms. An app given
 * as a string, or a test that passes `LooseAppTypes` as the type argument,
 * gets {@link LooseAppTypes}: any table, column and relation.
 */
import { createRequire } from "node:module";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { bindApp, boundApp, type App, type AppTypes, type BindAppOptions } from "../typed-app.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import type { JsonValue } from "../json-value.js";

/** A `jazz.app.json`, as an object; `format` defaults to 1. */
export interface TestAppFile {
  readonly format?: number;
  readonly tables: Readonly<Record<string, unknown>>;
  readonly [key: string]: unknown;
}

/** A `jazz.permissions.json`, as an object; `format` defaults to 1. */
export interface TestPermissionsFile {
  readonly format?: number;
  readonly tables: Readonly<Record<string, unknown>>;
  readonly defs?: Readonly<Record<string, unknown>>;
  readonly [key: string]: unknown;
}

/** One table of {@link LooseAppTypes}: any column, any relation. */
export interface LooseTableTypes {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  row: { readonly id: string; [column: string]: any };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  insert: { [column: string]: any };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  // `any`, not a union of kinds, so every column takes every where operator.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columns: { [column: string]: any };
  relations: { [relation: string]: { many: string; nullable?: boolean } };
}

/** The types of an app a test defines inline: every table, column and relation allowed. */
export interface LooseAppTypes extends AppTypes {
  [table: string]: LooseTableTypes;
}

// ============================================================================
// Inferred types: what `jazz-tools generate` writes for an app, from its file
// ============================================================================
//
// These mirror the TypeScript back end of `crates/jazz-codegen` (`ts.rs`):
// `tests/typed-app/infer-app-types.test.ts` checks them against the bindings
// it generated for a fixture with every column kind. A column type this
// can't read is loose: a table whose spec TS widened (a `string` where the
// file has a literal) is a {@link LooseTableTypes}, and a JSON column with a
// JSON Schema, whose generated type (`json_schema::to_ts`) isn't mirrored
// here, has an `any` value.

type Flat<T> = { [K in keyof T]: T[K] } & {};

type ScalarName =
  | "string"
  | "int"
  | "bigint"
  | "float"
  | "boolean"
  | "timestamp"
  | "uuid"
  | "bytes";

type ScalarValue<S extends ScalarName> = {
  string: string;
  uuid: string;
  int: number;
  float: number;
  bigint: bigint;
  boolean: boolean;
  timestamp: Date;
  bytes: Uint8Array;
}[S];

/** A column type as the loader reads it (`read_type`), in TS terms. */
interface ParsedType {
  readonly kind: string;
  /** The row form. */
  readonly value: unknown;
  /** The insert form (payload fields that may be left out are optional). */
  readonly init: unknown;
  readonly nullable: boolean;
}

/** A JSON column with a JSON Schema: its value is `any`, as in a loose app. */
interface SchemaJsonType {
  kind: "json";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  init: any;
  nullable: false;
}

type ParseType<S> = S extends `${infer B extends ScalarName}?`
  ? { kind: B; value: ScalarValue<B>; init: ScalarValue<B>; nullable: true }
  : S extends ScalarName
    ? { kind: S; value: ScalarValue<S>; init: ScalarValue<S>; nullable: false }
    : S extends { readonly type: infer T }
      ? ParseType<T>
      : S extends { readonly enum: readonly (infer V extends string)[] }
        ? { kind: "enum"; value: V; init: V; nullable: false }
        : S extends { readonly enum: infer Cases extends object }
          ? {
              kind: "enum";
              value: Payload<Cases, false>;
              init: Payload<Cases, true>;
              nullable: false;
            }
          : S extends { readonly json: true }
            ? { kind: "json"; value: JsonValue; init: JsonValue; nullable: false }
            : S extends { readonly json: unknown }
              ? SchemaJsonType
              : S extends { readonly array: infer E }
                ? ParseType<E> extends infer P extends ParsedType
                  ? { kind: "array"; value: P["value"][]; init: P["init"][]; nullable: false }
                  : never
                : never;

/** A column's type with its `optional`. */
type ParseColumn<S> =
  ParseType<S> extends infer P extends ParsedType
    ? S extends { readonly optional: true }
      ? { kind: P["kind"]; value: P["value"]; init: P["init"]; nullable: true }
      : P
    : never;

type OrNull<T, TNullable> = TNullable extends true ? T | null : T;

/** Whether an insert may leave the column (or payload field) out. */
type OptionalOnInsert<S> = ParseColumn<S>["nullable"] extends true
  ? true
  : S extends { readonly default: unknown }
    ? true
    : false;

type FieldsOf<T> = T extends object ? { [K in keyof T & string]: T[K] } : {};

type InsertFields<TFields, TValue extends Record<keyof TFields, unknown>> = Flat<
  {
    -readonly [K in keyof TFields as OptionalOnInsert<TFields[K]> extends true
      ? never
      : K]: TValue[K];
  } & {
    -readonly [K in keyof TFields as OptionalOnInsert<TFields[K]> extends true
      ? K
      : never]?: TValue[K];
  }
>;

/** A payload enum's union: `{ type: <case>; …fields }` per case. */
type Payload<TCases, TInit extends boolean> = {
  [C in keyof TCases & string]: TInit extends true
    ? Flat<
        { type: C } & InsertFields<
          FieldsOf<TCases[C]>,
          {
            [F in keyof FieldsOf<TCases[C]>]: OrNull<
              ParseColumn<FieldsOf<TCases[C]>[F]>["init"],
              ParseColumn<FieldsOf<TCases[C]>[F]>["nullable"]
            >;
          }
        >
      >
    : Flat<
        { type: C } & {
          -readonly [F in keyof FieldsOf<TCases[C]>]: OrNull<
            ParseColumn<FieldsOf<TCases[C]>[F]>["value"],
            ParseColumn<FieldsOf<TCases[C]>[F]>["nullable"]
          >;
        }
      >;
}[keyof TCases & string];

type TransformsOf<TApp> = TApp extends { readonly transforms: infer T extends object } ? T : {};

/** The type code reads and writes: its transform's view type, or its own. */
type ViewOf<TApp, S> = S extends { readonly transform: infer N extends keyof TransformsOf<TApp> }
  ? TransformsOf<TApp>[N] extends { readonly view: infer V }
    ? ParseType<V>
    : never
  : ParseColumn<S>;

type ColumnsOfSpec<T> = T extends { readonly columns: infer C extends object } ? FieldsOf<C> : {};
type ComputedOfSpec<T> = T extends { readonly computed: infer C extends object } ? FieldsOf<C> : {};
type RelationsOfSpec<T> = T extends { readonly relations: infer R extends object }
  ? FieldsOf<R>
  : {};

type RowValue<TApp, S> = OrNull<ViewOf<TApp, S>["value"], ParseColumn<S>["nullable"]>;

type ComputedValue<S> = S extends { readonly type: infer T }
  ? OrNull<ParseColumn<T>["value"], ParseColumn<T>["nullable"]>
  : // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any;

type InferRelation<TColumns, R> = R extends {
  readonly rel: infer Target extends string;
  readonly column: infer C extends string;
}
  ? C extends keyof TColumns
    ? {
        [K in ParseColumn<TColumns[C]>["kind"] extends "array" ? "many" : "one"]: Target;
      } & { nullable: ParseColumn<TColumns[C]>["nullable"] }
    : { one: Target; nullable: boolean }
  : R extends { readonly reverse: infer Target extends string }
    ? { many: Target }
    : { many: string; nullable?: boolean };

type InferComputed<TApp, TColumns, TComputed> = {
  [F in keyof TComputed]: {
    from: TComputed[F] extends { readonly from: readonly (infer C extends string)[] }
      ? Flat<{ [K in C]: K extends keyof TColumns ? RowValue<TApp, TColumns[K]> : string }>
      : {};
    value: ComputedValue<TComputed[F]>;
  };
};

type InferTable<TApp, TTable> = Flat<
  {
    row: Flat<
      { readonly id: string } & {
        -readonly [K in keyof ColumnsOfSpec<TTable>]: RowValue<TApp, ColumnsOfSpec<TTable>[K]>;
      } & {
        readonly [K in keyof ComputedOfSpec<TTable>]: ComputedValue<ComputedOfSpec<TTable>[K]>;
      }
    >;
    insert: InsertFields<
      ColumnsOfSpec<TTable>,
      {
        [K in keyof ColumnsOfSpec<TTable>]: OrNull<
          ViewOf<TApp, ColumnsOfSpec<TTable>[K]>["init"],
          ParseColumn<ColumnsOfSpec<TTable>[K]>["nullable"]
        >;
      }
    >;
    columns: {
      -readonly [K in keyof ColumnsOfSpec<TTable>]: {
        kind: ParseColumn<ColumnsOfSpec<TTable>[K]>["kind"];
        nullable: ParseColumn<ColumnsOfSpec<TTable>[K]>["nullable"];
        value: ParseColumn<ColumnsOfSpec<TTable>[K]>["value"];
      };
    };
    relations: {
      -readonly [K in keyof RelationsOfSpec<TTable>]: Flat<
        InferRelation<ColumnsOfSpec<TTable>, RelationsOfSpec<TTable>[K]>
      >;
    };
  } & (keyof ComputedOfSpec<TTable> extends never
    ? {}
    : { computed: InferComputed<TApp, ColumnsOfSpec<TTable>, ComputedOfSpec<TTable>> }) &
    (keyof TransformedColumns<TTable> extends never
      ? {}
      : { transforms: TransformedColumns<TTable> })
>;

/**
 * Whether TS widened a literal of the spec to `string` (a table written into
 * a `let`, or a `const` without `as const`), so its types can't be read: the
 * table is then a loose one.
 */
type Widened<S> = S extends string
  ? string extends S
    ? true
    : false
  : S extends readonly (infer E)[]
    ? Widened<E>
    : S extends object
      ? { [K in keyof S]-?: Widened<S[K]> }[keyof S]
      : false;

type TransformedColumns<TTable> = {
  -readonly [K in keyof ColumnsOfSpec<TTable> as ColumnsOfSpec<TTable>[K] extends {
    readonly transform: string;
  }
    ? K
    : never]: ColumnsOfSpec<TTable>[K] extends { readonly transform: infer N } ? N : never;
};

/**
 * The `Types` map `jazz-tools generate` writes into `jazz.generated.ts` for
 * the `jazz.app.json` `TApp` (an object literal, read with `const`).
 */
export type InferAppTypes<TApp> = TApp extends { readonly tables: infer T extends object }
  ? string extends keyof T
    ? LooseAppTypes
    : {
        -readonly [K in keyof T & string]: string extends keyof ColumnsOfSpec<T[K]>
          ? LooseTableTypes
          : true extends Widened<T[K]> | Widened<TransformsOf<TApp>>
            ? LooseTableTypes
            : InferTable<TApp, T[K]>;
      }
  : LooseAppTypes;

/** The `Transforms` map of `jazz.generated.ts` for `TApp`. */
export type InferTransformTypes<TApp> = {
  -readonly [N in keyof TransformsOf<TApp>]: TransformsOf<TApp>[N] extends {
    readonly stored: infer S;
    readonly view: infer V;
  }
    ? { stored: ParseType<S>["value"]; view: ParseType<V>["value"] }
    : never;
};

interface Napi {
  compileApp(appJson: string): string;
  loadBackendApp(schema: string, permissions: string): unknown;
}

let napi: Napi | undefined;

function loadNapi(): Napi {
  napi ??= createRequire(import.meta.url)("jazz-napi") as Napi;
  return napi;
}

function fileText(file: TestAppFile | TestPermissionsFile | string): string {
  return typeof file === "string" ? file : JSON.stringify({ format: 1, ...file });
}

/** What the bindings of `app` would pass to `bindApp`. */
export function compileTestApp(app: TestAppFile | string): BindAppOptions<AppTypes> {
  return JSON.parse(loadNapi().compileApp(fileText(app))) as BindAppOptions<AppTypes>;
}

/**
 * An app from an inline `jazz.app.json`, bound as its generated bindings
 * would bind it, typed as they would type it ({@link InferAppTypes}).
 * `implementations` gives the computed fields and transforms the file
 * declares (what `jazz.computed.ts` exports). Pass `LooseAppTypes` (or other
 * types) as the type argument to opt out of the inferred types.
 */
export function testApp<const TApp extends TestAppFile>(
  app: TApp,
  implementations?: TestAppImplementations,
): App<InferAppTypes<TApp>>;
export function testApp<TTypes extends AppTypes = LooseAppTypes>(
  app: TestAppFile | string,
  implementations?: TestAppImplementations,
): App<TTypes>;
export function testApp(
  app: TestAppFile | string,
  implementations: TestAppImplementations = {},
): unknown {
  return bindApp({ ...compileTestApp(app), ...implementations } as never);
}

/** What `jazz.computed.ts` would export for a {@link testApp}. */
export interface TestAppImplementations {
  computed?: Record<string, Record<string, (row: never) => unknown>>;
  transforms?: Record<string, { from(value: never): unknown; to(value: never): unknown }>;
}

/**
 * The engine's permissions for an inline `jazz.permissions.json`, checked
 * against `app` as a backend checks them, for a test that opens a context
 * or server directly. `createJazz`, `startTestJazz` and
 * `createPolicyTestApp` take the file itself.
 */
export function testPermissions(
  app: SchemaSourceInput,
  permissions: TestPermissionsFile | string,
): CompiledPermissionsMap {
  return loadTestPermissions(app, permissions).permissions;
}

/** {@link testPermissions}, with the bundle and the loader's warnings. */
export function loadTestPermissions(
  app: SchemaSourceInput,
  permissions: TestPermissionsFile | string,
): { permissions: CompiledPermissionsMap; bundle: string; warnings: string[] } {
  const bound = boundOf(app);
  return loadNapi().loadBackendApp(
    JSON.stringify({ schema: bound.schema, relations: bound.relations }),
    fileText(permissions),
  ) as { permissions: CompiledPermissionsMap; bundle: string; warnings: string[] };
}

function boundOf(app: SchemaSourceInput) {
  const bound = boundApp(resolveSchemaSource(app));
  if (!bound) throw new Error("testPermissions: `app` must come from testApp or jazz.generated.ts");
  return bound;
}
