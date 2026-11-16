/**
 * Root-level exports of the alpha.64/65 schema, permissions and migrations
 * DSL that alpha.66's TS authoring replaced (`schema.ts` with
 * `s.defineSchema` and the schema's own `definePermissions`, compiled by
 * `npx jazz-tools generate` to `jazz.app.json`, `jazz.permissions.json` and
 * `jazz.generated.ts`). The names stay as stubs that throw when used, naming
 * the replacement and pointing at `jazz-tools upgrade`. Without them a
 * bundler's "not exported" error would guess a similarly named export.
 */
import { type RemovedApi } from "./removed-api.js";

/** Where the upgrade from alpha.64 is written up (the alpha.65 section follows it). */
export const UPGRADE_FROM_ALPHA_64_URL =
  "https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha64";

const NO_REPLACEMENT =
  "it has no replacement: schema.ts defines the app with `s.defineSchema({ … })`, and " +
  "`npx jazz-tools generate` writes jazz.app.json, jazz.permissions.json and jazz.generated.ts from it. " +
  "Run `npx jazz-tools upgrade` to convert an alpha.64 or alpha.65 app";

/**
 * What replaces each alpha.64 name in alpha.66's TS authoring (`schema.ts`
 * with `s.defineSchema` and `schema.definePermissions`). Names not here
 * have no replacement.
 */
const AUTHORING_REPLACEMENTS: Readonly<Record<string, string>> = {
  defineApp:
    "define the schema with `export const schema = s.defineSchema({ … })` in schema.ts; runtime code imports `app` from ./jazz.generated",
  "schema.defineApp":
    "define the schema with `export const schema = s.defineSchema({ … })` in schema.ts; runtime code imports `app` from ./jazz.generated",
  defineSliceableApp:
    "schema slicing is gone: define the schema with `export const schema = s.defineSchema({ … })` and import `app` from ./jazz.generated",
  "schema.defineSliceableApp":
    "schema slicing is gone: define the schema with `export const schema = s.defineSchema({ … })` and import `app` from ./jazz.generated",
  defineSchema: 'use `s.defineSchema({ … })` (`import { schema as s } from "jazz-tools"`)',
  definePermissions:
    "permissions use the new DSL: `const { definePermissions } = schema; export const permissions = definePermissions({ <table>: { read, insert, update, delete } })`",
  "schema.definePermissions":
    "permissions use the new DSL: `const { definePermissions } = schema; export const permissions = definePermissions({ <table>: { read, insert, update, delete } })`",
  defineMigration:
    'use `migration.defineMigration({ … })` (`import { migration as m } from "jazz-tools"`)',
  renameTableFrom:
    'use `migration.renameTableFrom(…)` (`import { migration as m } from "jazz-tools"`)',
  table: "use `s.table(columns, relations)` inside `s.defineSchema({ … })`",
  anyOf: "use the schema's own `anyOf` (`const { anyOf } = schema`)",
  allOf: "use the schema's own `allOf` (`const { allOf } = schema`)",
  createSessionContext: "use the schema's own `session` (`const { session } = schema`)",
};

/** The error a removed DSL export throws. `code` is `JAZZ_API_REMOVED`. */
export function removedSchemaApiError(name: string): Error {
  const replacement = AUTHORING_REPLACEMENTS[name];
  const message = replacement
    ? `[JAZZ_API_REMOVED] ${name} was removed: ${replacement}. Run \`npx jazz-tools upgrade\` to convert an alpha.64 or alpha.65 app. See ${UPGRADE_FROM_ALPHA_64_URL}`
    : `[JAZZ_API_REMOVED] ${name} was removed: ${NO_REPLACEMENT}. See ${UPGRADE_FROM_ALPHA_64_URL}`;
  return Object.assign(new Error(message), { code: "JAZZ_API_REMOVED" });
}

/** @internal A stub for a removed DSL export: throws when called or constructed. */
export function removedSchemaApi(name: string): RemovedApi {
  // A plain function (not a Proxy): React and Fast Refresh inspect exports,
  // which must stay inert. Calling or constructing it throws.
  const stub = function removedJazzSchemaApi(): never {
    throw removedSchemaApiError(name);
  };
  Object.defineProperty(stub, "name", { value: name });
  return stub as unknown as RemovedApi;
}

/**
 * What `s.enum({ ...cases })` throws: enum cases with fields are gone for
 * good, not just moved to jazz.app.json (jazz#5758).
 */
export const ENUM_CASES_NOT_SUPPORTED =
  'enum cases with fields are not supported; use s.enum("a", "b") plus a nullable column per field, or s.json() (see https://github.com/garden-co/jazz/issues/5758)';

/** `schema.enum`: like every removed member, except that the object form names its own refusal. */
function removedEnum(): RemovedApi {
  const stub = function removedJazzSchemaApi(...args: unknown[]): never {
    if (args.length === 1 && typeof args[0] === "object" && args[0] !== null) {
      throw new Error(ENUM_CASES_NOT_SUPPORTED);
    }
    throw removedSchemaApiError("schema.enum");
  };
  Object.defineProperty(stub, "name", { value: "schema.enum" });
  return stub as unknown as RemovedApi;
}

/** A removed namespace: each member it had is a stub that throws, naming itself. */
function removedNamespace<const TMembers extends readonly string[]>(
  namespace: string,
  members: TMembers,
): { readonly [K in TMembers[number]]: RemovedApi } {
  const stubs: Record<string, RemovedApi> = {};
  for (const member of members)
    stubs[member] =
      namespace === "schema" && member === "enum"
        ? removedEnum()
        : removedSchemaApi(`${namespace}.${member}`);
  return Object.freeze(stubs) as { readonly [K in TMembers[number]]: RemovedApi };
}

/**
 * @deprecated Not available here, and throws when used: the schema DSL is for
 * authoring only. Define the app in schema.ts (`s.defineSchema`), run
 * `npx jazz-tools generate`, and import `app` from ./jazz.generated at runtime.
 */
export const schema = /*#__PURE__*/ removedNamespace("schema", [
  "table",
  "computed",
  "string",
  "uuid",
  "boolean",
  "int",
  "bigint",
  "timestamp",
  "float",
  "bytes",
  "json",
  "enum",
  "array",
  "rel",
  "reverse",
  "defineApp",
  "defineSchema",
  "defineSliceableApp",
  "definePermissions",
  "allowExternalProvenanceName",
] as const);

/**
 * @deprecated Not available here, and throws when used: migrations are
 * authored in migrations/*.ts, written by `npx jazz-tools migrations create`.
 */
export const migration = /*#__PURE__*/ removedNamespace("migration", [
  "add",
  "drop",
  "renameFrom",
  "defineMigration",
  "renameTableFrom",
] as const);

/** @deprecated Removed, and throws when used: define the schema in schema.ts with `export const schema = s.defineSchema({ … })`; import `app` from ./jazz.generated. Run `npx jazz-tools upgrade`. */
export const defineApp: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineApp");
/** @deprecated Removed, and throws when used: use `s.defineSchema({ … })` (`import { schema as s } from "jazz-tools"`). Run `npx jazz-tools upgrade`. */
export const defineSchema: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineSchema");
/** @deprecated Removed, and throws when used: schema slicing is gone; use `s.defineSchema({ … })` and the generated `app`. Run `npx jazz-tools upgrade`. */
export const defineSliceableApp: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineSliceableApp");
/** @deprecated Removed, and throws when used: use the schema's own `definePermissions` (`const { definePermissions } = schema`). Run `npx jazz-tools upgrade`. */
export const definePermissions: RemovedApi = /*#__PURE__*/ removedSchemaApi("definePermissions");
/** @deprecated Removed, and throws when used: use `m.defineMigration` (`import { migration as m } from "jazz-tools"`). Run `npx jazz-tools upgrade`. */
export const defineMigration: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineMigration");
/** @deprecated Removed, and throws when used: use `m.renameTableFrom` (`import { migration as m } from "jazz-tools"`). Run `npx jazz-tools upgrade`. */
export const renameTableFrom: RemovedApi = /*#__PURE__*/ removedSchemaApi("renameTableFrom");
/** @deprecated Removed, and throws when used: no replacement: jazz.generated.ts embeds the compiled schema. Run `npx jazz-tools upgrade`. */
export const schemaToWasm: RemovedApi = /*#__PURE__*/ removedSchemaApi("schemaToWasm");
/** @deprecated Removed, and throws when used: use `s.table(columns, relations)` inside `s.defineSchema({ … })`. Run `npx jazz-tools upgrade`. */
export const table: RemovedApi = /*#__PURE__*/ removedSchemaApi("table");
/** @deprecated Removed, and throws when used: use the schema's own `anyOf` (`const { anyOf } = schema`). Run `npx jazz-tools upgrade`. */
export const anyOf: RemovedApi = /*#__PURE__*/ removedSchemaApi("anyOf");
/** @deprecated Removed, and throws when used: use the schema's own `allOf` (`const { allOf } = schema`). Run `npx jazz-tools upgrade`. */
export const allOf: RemovedApi = /*#__PURE__*/ removedSchemaApi("allOf");
