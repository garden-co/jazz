/**
 * The permissions DSL's types: handles for rows, columns, relations and
 * session values, the condition objects a table takes, and the checks
 * `definePermissions` runs on what it is given (every table listed, only the
 * six rule keys, `update` apart from `updateIf`/`updateTo`, known columns
 * and relations, values of the column's type).
 */
import type {
  AnyColumnDefinition,
  AnyTypedColumnBuilder,
  ColumnBuilderOptional,
  ColumnBuilderSqlType,
  ColumnBuilderValue,
} from "../dsl.js";
import type { Relationships } from "../relationships.js";
import type { DefinedTable, StoredColumnKeys } from "../schema-dsl/table.js";

// ============================================================================
// Brands
// ============================================================================

declare const valueBrand: unique symbol;
declare const columnBrand: unique symbol;
declare const oldBrand: unique symbol;
declare const policyBrand: unique symbol;
declare const relBrand: unique symbol;
declare const opBrand: unique symbol;
declare const inheritsBrand: unique symbol;

/**
 * A value a condition can compare with: a column of a row, a session value
 * or a typed literal. `V` is its TS type, `K` its kind (`"uuid"`, `"string"`,
 * …), which must match the column it is compared with.
 */
export interface ValueHandle<V = unknown, K = string> {
  readonly [valueBrand]: { readonly value: V; readonly kind: K };
}

/** A column of a row (`row.chatId`), or of a table (`schema.tables.t.col`, for `allowedTo` `via`). */
export interface ColumnHandle<
  V = unknown,
  K = string,
  T extends string = string,
> extends ValueHandle<V, K> {
  readonly [columnBrand]: T;
}

/** A column of the row before an update (`updateTo`'s `old`): only for direct comparisons. */
export interface OldValue<V = unknown, K = string> {
  readonly [oldBrand]: { readonly value: V; readonly kind: K };
}

/** The kinds of values, as `jazz.app.json` names column types. */
export type ValueKind =
  | "string"
  | "uuid"
  | "boolean"
  | "int"
  | "float"
  | "bigint"
  | "timestamp"
  | "bytes"
  | "json"
  | "enum"
  | "array";

/** The kinds a value of kind `K` can be compared with. */
export type CompatibleKind<K> = K extends "int" | "float" ? "int" | "float" : K;

type LiteralOfKind<K> = K extends "string" | "uuid" | "enum"
  ? string
  : K extends "int" | "float"
    ? number
    : K extends "bigint"
      ? bigint | number
      : K extends "boolean"
        ? boolean
        : K extends "timestamp"
          ? Date | number
          : K extends "bytes"
            ? Uint8Array
            : K extends "array"
              ? readonly unknown[]
              : K extends "json"
                ? unknown
                : never;

/**
 * Any value of kind `K` (`"uuid"`, `"string"`, …), for helper parameters: a
 * literal, a row's column of that kind, a session value or a claim.
 */
export type Value<K extends ValueKind = ValueKind> =
  | LiteralOfKind<K>
  | ValueHandle<any, CompatibleKind<K>>;

/** Any uuid-typed value: a literal, a uuid column of a row, `session.user`, a claim. */
export type Uuid = Value<"uuid">;

/**
 * A policy: what a rule allows. `X` collects the plain condition objects
 * written at its top (`anyOf({ isPublic: true }, …)`), which only the rule
 * using it can check against its table.
 */
export interface Policy<X = never> {
  readonly [policyBrand]: { readonly conditions: X };
}

/**
 * What `allowedTo.<op>(…)` gives, and `anyOf` / `allOf` of it: a policy a
 * rule takes, but neither one inside an `exists` (its conditions, a hop's,
 * a relation's `.where`) nor one under `not`, where inheritance is not
 * supported yet.
 */
export interface Inherits {
  readonly [inheritsBrand]: true;
}

/** A policy an `exists` takes in its conditions: any but one inheriting with `allowedTo`. */
export type NestedPolicy<X = any> = Policy<X> & { readonly [inheritsBrand]?: never };

/**
 * `Inherits` when any of the inputs `I` (a union) inherits, else `unknown`.
 * An `any` input (a helper's untyped value) counts as not inheriting.
 */
export type InheritsIn<I> = 0 extends 1 & I
  ? unknown
  : [Extract<I, Inherits>] extends [never]
    ? unknown
    : Inherits;

/** A comparison helper's value for a condition object: `{ joinCode: ne(null) }`. */
export interface CompareOp<X = unknown> {
  readonly [opBrand]: { readonly operand: X; readonly op: "ne" | "gt" | "gte" | "lt" | "lte" };
}
/** `contains(v)`: an array column holds `v`, or a text column contains it. */
export interface ContainsOp<X = unknown> {
  readonly [opBrand]: { readonly operand: X; readonly op: "contains" };
}
/** `isIn(session.claims.teams)`: the column is one of a session list. */
export interface InOp {
  readonly [opBrand]: { readonly operand: unknown; readonly op: "in" };
}

/**
 * A relation of a row (`row.chat`, `row.chatMembersViaChat`). `C` is the
 * condition object its target takes: `.where(C)` is `exists` on it, and
 * `allowedTo.read(rel)` inherits from it.
 */
export interface Rel<C = any> {
  readonly [relBrand]: true;
  /** The rows this relation leads to, filtered by `conditions`. */
  where(conditions?: C): Policy;
}

// ============================================================================
// Schema shapes
// ============================================================================

/** The tables `s.defineSchema` takes. */
export type SchemaTables = Record<string, DefinedTable<any, any>>;

type ColumnsOf<S, T extends keyof S> = S[T] extends DefinedTable<infer C, any> ? C : never;
type RelationsOf<S, T extends keyof S> =
  S[T] extends DefinedTable<any, infer R> ? (R extends Relationships ? R : {}) : {};
type StoredOf<S, T extends keyof S> = StoredColumnKeys<ColumnsOf<S, T>>;

/** The table a relation of `T` leads to. */
export type RelationTarget<S, T extends keyof S, R> = R extends keyof RelationsOf<S, T>
  ? RelationsOf<S, T>[R] extends { readonly table: infer X extends string }
    ? X
    : never
  : never;

/** A column's kind, which a value compared with it must share. */
export type KindOf<Sql> = Sql extends "TEXT"
  ? "string"
  : Sql extends "UUID"
    ? "uuid"
    : Sql extends "BOOLEAN"
      ? "boolean"
      : Sql extends "INTEGER"
        ? "int"
        : Sql extends "REAL"
          ? "float"
          : Sql extends "BIGINT"
            ? "bigint"
            : Sql extends "TIMESTAMP"
              ? "timestamp"
              : Sql extends "BYTEA"
                ? "bytes"
                : Sql extends { kind: "ENUM" }
                  ? "enum"
                  : Sql extends { kind: "JSON" }
                    ? "json"
                    : Sql extends { kind: "ARRAY" }
                      ? "array"
                      : string;

/** A literal for a column of value type `V` and kind `K`. */
export type LiteralFor<V, K> = K extends "timestamp"
  ? Date | number
  : K extends "bigint"
    ? bigint | number
    : V;

type BuilderValue<B> = B extends AnyTypedColumnBuilder ? ColumnBuilderValue<B> : never;
type BuilderKind<B> = B extends AnyTypedColumnBuilder ? KindOf<ColumnBuilderSqlType<B>> : never;
type BuilderOptional<B> = B extends AnyTypedColumnBuilder ? ColumnBuilderOptional<B> : false;

/** What a column can be compared with: a literal or a handle of its kind. */
export type Operand<V, K> = LiteralFor<V, K> | ValueHandle<V, CompatibleKind<K>>;

/**
 * The value a condition object gives a column: a literal or handle (`=`),
 * an array (`in`), `null` (is null), a comparison helper, or (in `updateTo`)
 * a column of the old row.
 */
export type ConditionValue<V, K, Nullable, Old = never> =
  | Operand<V, K>
  | readonly Operand<V, K>[]
  | (Nullable extends true ? null : never)
  | CompareOp<Operand<V, K> | null | Old>
  | ContainsOp<unknown>
  | InOp
  | Old;

/** Value, kind and nullability of every column a condition on `T` can name. */
export type ColumnInfo<S, T extends keyof S> = {
  [C in StoredOf<S, T>]: {
    value: BuilderValue<ColumnsOf<S, T>[C]>;
    kind: BuilderKind<ColumnsOf<S, T>[C]>;
    nullable: BuilderOptional<ColumnsOf<S, T>[C]>;
  };
} & {
  id: { value: string; kind: "uuid"; nullable: false };
} & MagicColumnInfo;

type MagicColumnInfo = {
  $createdBy: { value: string; kind: "uuid"; nullable: false };
  "$createdBy.account": { value: string; kind: "uuid"; nullable: false };
  "$createdBy.identity": { value: string; kind: "identity"; nullable: false };
  $updatedBy: { value: string; kind: "uuid"; nullable: false };
  "$updatedBy.account": { value: string; kind: "uuid"; nullable: false };
  "$updatedBy.identity": { value: string; kind: "identity"; nullable: false };
  $deletedBy: { value: string; kind: "uuid"; nullable: true };
  "$deletedBy.account": { value: string; kind: "uuid"; nullable: true };
  "$deletedBy.identity": { value: string; kind: "identity"; nullable: true };
  $createdAt: { value: Date; kind: "timestamp"; nullable: false };
  $updatedAt: { value: Date; kind: "timestamp"; nullable: false };
  $deletedAt: { value: Date; kind: "timestamp"; nullable: true };
};

/** The condition object of table `T`: `{ column: value }`, AND-ed. */
export type Conditions<S, T extends keyof S, WithOld extends boolean = false> = {
  readonly [C in keyof ColumnInfo<S, T>]?: ConditionValue<
    ColumnInfo<S, T>[C]["value"],
    ColumnInfo<S, T>[C]["kind"],
    ColumnInfo<S, T>[C]["nullable"],
    WithOld extends true
      ? OldValue<ColumnInfo<S, T>[C]["value"], ColumnInfo<S, T>[C]["kind"]>
      : never
  >;
};

/** A row of `T`: its columns, `id`, its magic columns and its relations, as handles. */
export type Row<S, T extends keyof S> = {
  readonly [C in keyof ColumnInfo<S, T>]: ColumnHandle<
    ColumnInfo<S, T>[C]["value"],
    ColumnInfo<S, T>[C]["kind"],
    T & string
  >;
} & {
  readonly [R in keyof RelationsOf<S, T>]: Rel<Conditions<S, RelationTarget<S, T, R> & keyof S>>;
};

/** The row before an update: its columns, for direct comparisons only. */
export type OldRow<S, T extends keyof S> = {
  readonly [C in keyof ColumnInfo<S, T>]: OldValue<
    ColumnInfo<S, T>[C]["value"],
    ColumnInfo<S, T>[C]["kind"]
  >;
};

/** `schema.tables.<t>`: the table's definition and its columns as handles (`allowedTo.read(schema.tables.t.col)`). */
export type TableHandles<S> = {
  readonly [T in keyof S]: S[T] & {
    readonly [C in StoredOf<S, T>]: ColumnHandle<
      ColumnInfo<S, T>[C]["value"],
      ColumnInfo<S, T>[C]["kind"],
      T & string
    >;
  };
};

// ============================================================================
// Policies and rules
// ============================================================================

/** Anything a policy helper or rule takes: a policy, a condition object, `true`/`false`. */
export type PolicyInput = Policy<any> | boolean | { readonly [column: string]: unknown };

/** The condition objects written at a policy's top (unchecked until a rule uses it). */
export type ConditionsIn<I> =
  I extends Policy<infer X> ? X : I extends boolean ? never : I extends object ? I : never;

type RuleInput<S, T extends keyof S> =
  | boolean
  | Policy<any>
  | Conditions<S, T>
  | ((row: Row<S, T>) => PolicyInput);

/** What `definePermissions` takes for one table. Every missing operation is denied. */
export interface RulesShape<S, T extends keyof S> {
  read?: RuleInput<S, T>;
  insert?: RuleInput<S, T>;
  /** The same check before and after an update. Not with `updateIf`/`updateTo`. */
  update?: RuleInput<S, T>;
  /** Which rows may be updated (checked on the row before the update). Needs `updateTo`. */
  updateIf?: RuleInput<S, T>;
  /** What a row may be updated to; `old` is the row before, for direct comparisons. Needs `updateIf`. */
  updateTo?:
    | boolean
    | Policy<any>
    | Conditions<S, T>
    | ((row: Row<S, T>, old: OldRow<S, T>) => PolicyInput);
  delete?: RuleInput<S, T>;
}

/** What `definePermissions` takes: every table of the schema. */
export type PermissionsShape<S> = { [T in keyof S]: RulesShape<S, T> };

/** A refusal `definePermissions`' types report. */
export interface PermissionError<Message extends string> {
  readonly "jazz permissions error": Message;
}

type UnknownKeys<S, T extends keyof S, O> = Exclude<keyof O, keyof ColumnInfo<S, T>> & string;
type WrongValueKeys<S, T extends keyof S, O, WithOld extends boolean> = {
  [C in keyof O & keyof ColumnInfo<S, T>]: O[C] extends Conditions<S, T, WithOld>[C] ? never : C;
}[keyof O & keyof ColumnInfo<S, T>] &
  string;

type CheckObject<S, T extends keyof S, O, WithOld extends boolean> = [
  UnknownKeys<S, T, O>,
] extends [never]
  ? [WrongValueKeys<S, T, O, WithOld>] extends [never]
    ? never
    : PermissionError<`wrong value type for ${T & string}.${WrongValueKeys<S, T, O, WithOld>}`>
  : PermissionError<`unknown column ${UnknownKeys<S, T, O>} in ${T & string}`>;

/** The errors of the condition objects `X` written at a rule's top, or never. */
type ConditionErrors<S, T extends keyof S, X, WithOld extends boolean> = [X] extends [never]
  ? never
  : X extends unknown
    ? CheckObject<S, T, X, WithOld>
    : never;

/** `unknown` when a rule's result `X` is fine, else the error to show. */
type CheckRule<S, T extends keyof S, X, WithOld extends boolean> = [
  ConditionErrors<S, T, ConditionsIn<X>, WithOld>,
] extends [never]
  ? unknown
  : ConditionErrors<S, T, ConditionsIn<X>, WithOld>;

type RuleKey = "read" | "insert" | "update" | "updateIf" | "updateTo" | "delete";

type Has<R, K extends string> = [Extract<keyof R, K>] extends [never] ? false : true;

type CheckRules<S, T extends keyof S, R> = {
  [K in keyof R]: K extends RuleKey
    ? K extends "update"
      ? Has<R, "updateIf" | "updateTo"> extends true
        ? PermissionError<"`update` can't be combined with `updateIf`/`updateTo`">
        : CheckRule<S, T, R[K], false>
      : K extends "updateIf"
        ? Has<R, "updateTo"> extends true
          ? CheckRule<S, T, R[K], false>
          : PermissionError<"`updateIf` needs `updateTo` (what a row may be updated to)">
        : K extends "updateTo"
          ? Has<R, "updateIf"> extends true
            ? CheckRule<S, T, R[K], true>
            : PermissionError<"`updateTo` needs `updateIf` (which rows may be updated)">
          : CheckRule<S, T, R[K], false>
    : PermissionError<`unknown rule key ${K & string}: use read, insert, update, updateIf, updateTo or delete`>;
};

/**
 * `definePermissions`' checks of the results `R` its rules give (per table,
 * per rule key: the policy, condition object or boolean a rule is or
 * returns): `unknown` where fine, an error to show where not.
 */
export type CheckPermissions<S, R> = {
  [T in keyof R]: T extends keyof S
    ? CheckRules<S, T, R[T]>
    : PermissionError<`unknown table ${T & string}`>;
};

/** One rule: a policy (or condition object, or boolean) `X`, or a function of the row giving one. */
export type RuleFor<S, T extends keyof S, K, X> = K extends "updateTo"
  ? X | ((row: Row<S, T>, old: OldRow<S, T>) => X)
  : X | ((row: Row<S, T>) => X);

/**
 * What `definePermissions` takes, written over the rules' results `R` so
 * TypeScript infers each rule's own result (the row parameters depend only
 * on the schema).
 */
export type PermissionsInput<S, R> = {
  [T in keyof R]: { [K in keyof R[T]]: RuleFor<S, T & keyof S, K, R[T][K]> };
};

/** Every table of `S`, each with any rules. */
export type AllTables<S> = { [T in keyof S]: unknown };

/** `jazz.permissions.json`'s `$schema`, which names its format version. */
export const PERMISSIONS_SCHEMA_URL =
  "https://jazz.tools/schemas/jazz.permissions.v1.json" as const;

/** The readable `jazz.permissions.json` tree `definePermissions` returns, under {@link READABLE_PERMISSIONS}. */
export interface ReadablePermissionsFile {
  $schema: typeof PERMISSIONS_SCHEMA_URL;
  tables: Record<string, Record<string, unknown>>;
}

/** The tag of `definePermissions`' result: `Symbol.for("jazz.permissions.readable")`. */
export const READABLE_PERMISSIONS: unique symbol = Symbol.for("jazz.permissions.readable") as never;

/** What `definePermissions` returns: the readable tree, read by `jazz-tools generate`. */
export interface Permissions<S = unknown> {
  readonly [READABLE_PERMISSIONS]: ReadablePermissionsFile;
  /** @internal The schema the permissions are for (types only). */
  readonly __schema?: S;
}

// ============================================================================
// Session
// ============================================================================

/** Untyped claims: any path, any value. */
export type AnyClaims = ValueHandle<any, any> & { readonly [path: string]: AnyClaims };

/** Typed claims: `s.claims<{ join_code: string; teams: string[] }>()`. */
export type TypedClaims<C> = {
  readonly [K in keyof C]: C[K] extends readonly unknown[]
    ? ValueHandle<C[K], any>
    : C[K] extends object
      ? ValueHandle<C[K], any> & TypedClaims<C[K]>
      : ValueHandle<C[K], any>;
};

/** A claims type marker for `s.defineSchema(tables, { claims: s.claims<…>() })`. */
export interface ClaimsMarker<C> {
  readonly __claims?: C;
}

/** The values of `session.authMode`. */
export type SessionAuthMode = "external" | "device-account" | "anonymous";

/** The session's values. */
export interface Session<Claims = AnyClaims> {
  /**
   * The account. `{ $createdBy: session.user }` matches rows written through
   * any identity linked to the account.
   */
  readonly user: ValueHandle<string, "uuid">;
  /** The exact login (issuer and subject): compare it with `$createdBy.identity`. */
  readonly identity: ValueHandle<string, "identity"> & {
    readonly subject: ValueHandle<string, any>;
    readonly issuer: ValueHandle<string, any>;
  };
  /**
   * How the session authenticated: `"external"`, `"device-account"` or
   * `"anonymous"`. Device accounts were `"local-first"` before 2026-11;
   * comparing with that is refused.
   */
  readonly authMode: ValueHandle<SessionAuthMode, any>;
  /** The provider's claims, `session.claims.<path>`. */
  readonly claims: Claims;
}

export type { AnyColumnDefinition };
