/**
 * The TypeScript schema, permissions and migrations DSL that alpha.65
 * removed: `jazz.app.json` and `jazz.permissions.json` are now an app's only
 * source, and `jazz.generated.ts` (written by `jazz-tools generate`) its
 * typed bindings. The names stay for one release as stubs that throw when
 * used, pointing at `jazz-tools upgrade`, which converts an alpha.64 app.
 * Without them a bundler's "not exported" error would guess a similarly
 * named export.
 */
import { type RemovedApi } from "./removed-api.js";

/** Where the alpha.64 → alpha.65 changes are written up. */
export const UPGRADE_FROM_ALPHA_64_URL =
  "https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha64";

const FILES_AS_SOURCE =
  "jazz.app.json and jazz.permissions.json are now the app's schema, migrations and permissions, " +
  "with typed bindings in jazz.generated.ts. Run `npx jazz-tools upgrade` to convert an alpha.64 app";

/** The error a removed DSL export throws. `code` is `JAZZ_API_REMOVED`. */
export function removedSchemaApiError(name: string): Error {
  return Object.assign(
    new Error(
      `[JAZZ_API_REMOVED] ${name} was removed in jazz-tools alpha.65: ${FILES_AS_SOURCE}. See ${UPGRADE_FROM_ALPHA_64_URL}`,
    ),
    { code: "JAZZ_API_REMOVED" },
  );
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
 * @deprecated Removed in alpha.65, and throws when used: jazz.app.json is the
 * app's schema, jazz.permissions.json its permissions. Run `npx jazz-tools upgrade`.
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
 * @deprecated Removed in alpha.65, and throws when used: migrations live in
 * jazz.app.json, written by `jazz-tools migrations create`. Run `npx jazz-tools upgrade`.
 */
export const migration = /*#__PURE__*/ removedNamespace("migration", [
  "add",
  "drop",
  "renameFrom",
  "defineMigration",
  "renameTableFrom",
] as const);

/** @deprecated Removed in alpha.65, and throws when used: import `app` from ./jazz.generated. Run `npx jazz-tools upgrade`. */
export const defineApp: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineApp");
/** @deprecated Removed in alpha.65, and throws when used: jazz.app.json is the app's schema. Run `npx jazz-tools upgrade`. */
export const defineSchema: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineSchema");
/** @deprecated Removed in alpha.65, and throws when used: schema slicing is gone; use the generated `app`. Run `npx jazz-tools upgrade`. */
export const defineSliceableApp: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineSliceableApp");
/** @deprecated Removed in alpha.65, and throws when used: jazz.permissions.json holds the permissions. Run `npx jazz-tools upgrade`. */
export const definePermissions: RemovedApi = /*#__PURE__*/ removedSchemaApi("definePermissions");
/** @deprecated Removed in alpha.65, and throws when used: migrations live in jazz.app.json. Run `npx jazz-tools upgrade`. */
export const defineMigration: RemovedApi = /*#__PURE__*/ removedSchemaApi("defineMigration");
/** @deprecated Removed in alpha.65, and throws when used: migrations live in jazz.app.json. Run `npx jazz-tools upgrade`. */
export const renameTableFrom: RemovedApi = /*#__PURE__*/ removedSchemaApi("renameTableFrom");
/** @deprecated Removed in alpha.65, and throws when used: jazz.generated.ts embeds the compiled schema. Run `npx jazz-tools upgrade`. */
export const schemaToWasm: RemovedApi = /*#__PURE__*/ removedSchemaApi("schemaToWasm");
/** @deprecated Removed in alpha.65, and throws when used: jazz.app.json is the app's schema. Run `npx jazz-tools upgrade`. */
export const table: RemovedApi = /*#__PURE__*/ removedSchemaApi("table");
/** @deprecated Removed in alpha.65, and throws when used: permissions are written in jazz.permissions.json. Run `npx jazz-tools upgrade`. */
export const anyOf: RemovedApi = /*#__PURE__*/ removedSchemaApi("anyOf");
/** @deprecated Removed in alpha.65, and throws when used: permissions are written in jazz.permissions.json. Run `npx jazz-tools upgrade`. */
export const allOf: RemovedApi = /*#__PURE__*/ removedSchemaApi("allOf");
