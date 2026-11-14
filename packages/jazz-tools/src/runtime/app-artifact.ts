import type { WasmSchema } from "../drivers/types.js";

/**
 * An app's `jazz.app.json`: its schema versions, the migrations between them
 * and metadata for typed code, in one language-neutral file (SPEC 10.10). It
 * holds no permissions: those are in `jazz.permissions.json`
 * ({@link AppPermissions}), which only backends read, so no client bundle
 * carries them. `jazz-tools validate` and `jazz-tools migrations create`
 * write both beside schema.ts, even while the app has a single version; the
 * app commits them and passes this one to `defineApp`:
 *
 * ```ts
 * import artifact from "./jazz.app.json" with { type: "json" };
 * export const app = s.defineApp(schema, { artifact });
 * ```
 *
 * jazz-tools doesn't read the file itself: every runtime hands it to Jazz's
 * Rust reader, which refuses a file that doesn't check out. A client opens
 * along its schemas and migrations, so it stores every column where the
 * server does, even for a version it opens before it ever syncs. A backend
 * publishes its deployment at startup.
 */
export interface AppArtifact {
  readonly format: number;
  readonly current: string;
  readonly schemas: readonly unknown[];
  readonly migrations?: readonly unknown[];
  readonly codegen?: unknown;
}

/**
 * An app's `jazz.permissions.json`: the permissions of its current version
 * (`schema`) and the bundle id they make (SPEC 10.10). Only backends read
 * it: `createJazz({ app, permissions })` takes it as `permissions`,
 *
 * ```ts
 * import permissions from "./jazz.permissions.json" with { type: "json" };
 * const jazz = createJazz({ app, permissions, store });
 * ```
 *
 * and publishes exactly what it holds. Like `jazz.app.json`, Jazz's Rust
 * reader checks it, and refuses one that wasn't written with the app's file.
 */
export interface AppPermissions {
  readonly format: number;
  readonly schema: string;
  readonly permissions?: Readonly<Record<string, unknown>>;
  readonly bundle: string;
}

/** Where the app's `jazz.app.json` lives, beside schema.ts. */
export const APP_ARTIFACT_FILE = "jazz.app.json";

/** Where the app's `jazz.permissions.json` lives, beside schema.ts. */
export const APP_PERMISSIONS_FILE = "jazz.permissions.json";

/** How to give a backend its permissions: the fix its refusals name. */
export const SHIP_PERMISSIONS_HINT =
  'Pass the app\'s jazz.permissions.json: `import permissions from "./jazz.permissions.json" with { type: "json" };` ' +
  "then `createJazz({ app, permissions, ... })`. `npx jazz-tools validate` writes it from permissions.ts beside schema.ts.";

/**
 * Whether `value` is a `jazz.permissions.json` rather than the map
 * `definePermissions` makes. A table named `format` holds policies, never
 * a number, so the two can't be confused.
 */
export function isAppPermissions(value: unknown): value is AppPermissions {
  const candidate = value as Partial<Record<keyof AppPermissions, unknown>> | null;
  return (
    typeof candidate === "object" &&
    candidate !== null &&
    typeof candidate.format === "number" &&
    typeof candidate.schema === "string" &&
    typeof candidate.bundle === "string"
  );
}

/** How to give an app its `jazz.app.json`: the fix every refusal below names. */
export const SHIP_ARTIFACT_HINT =
  'In schema.ts, `import artifact from "./jazz.app.json" with { type: "json" };` and pass it: `s.defineApp(schema, { artifact })`. ' +
  "Then run `npx jazz-tools validate`, which writes jazz.app.json beside schema.ts, and commit the file.";

/**
 * Where the app's schema carries its `jazz.app.json`: a property on the schema
 * object itself, under a registered symbol, so that every copy of jazz-tools
 * in a process reads what `defineApp` recorded. A bundler can load the module
 * defining the app and the one opening the store as separate copies (Next's
 * server bundles do); state private to this module would then leave the
 * store opening the app without its history, as though its current schema
 * were its first. Not enumerable: the schema's JSON, and so its hash, stay as
 * they are.
 */
const SHIPPED = Symbol.for("jazz-tools.appArtifact");

/**
 * Where the app's schema records that `defineApp` was given
 * `{ migrations: "none" }`, read across copies of jazz-tools like
 * {@link SHIPPED}. It tells such an app apart from a schema that never went
 * through `defineApp`, so `createJazz` can refuse it on a persistent store.
 */
const NONE = Symbol.for("jazz-tools.noMigrations");

/** What `defineApp` takes besides the schema: see `DefineAppOptions`. */
export type ShippedApp = { artifact: AppArtifact } | { migrations: "none" };

/** Record what the app defining `schema` ships, refusing what isn't an app's. */
export function shipArtifact(schema: WasmSchema, options: ShippedApp | undefined): void {
  const given = options as { artifact?: unknown; migrations?: unknown } | undefined;
  if (given?.migrations === "none" && given.artifact === undefined) {
    define(schema, NONE, true);
    return;
  }
  if (given?.migrations !== undefined) {
    throw new Error(
      "defineApp no longer takes `{ migrations }`: since alpha.63 an app ships its jazz.app.json instead of migrations/index.ts. " +
        SHIP_ARTIFACT_HINT +
        ' `{ migrations: "none" }` stays for an app that only ever runs on memory(), such as a test\'s.',
    );
  }
  if (given?.artifact === undefined) {
    throw new Error(
      "defineApp needs the app's jazz.app.json: " +
        SHIP_ARTIFACT_HINT +
        ' Or pass `{ migrations: "none" }` for an app that only ever runs on memory(), such as a test\'s.',
    );
  }
  define(schema, SHIPPED, checkedArtifact(given.artifact));
}

/** `value`, if it looks like a `jazz.app.json`; Rust checks the rest. */
function checkedArtifact(value: unknown): AppArtifact {
  const candidate = value as Partial<Record<keyof AppArtifact, unknown>> & { default?: unknown };
  if (
    typeof candidate === "object" &&
    candidate !== null &&
    typeof candidate.format === "number" &&
    typeof candidate.current === "string" &&
    Array.isArray(candidate.schemas)
  ) {
    if ("permissions" in candidate || "bundle" in candidate) {
      // Clients ship this file: one that still holds the permissions would
      // put them in every client bundle.
      throw new Error(
        "defineApp was given a jazz.app.json that still holds the app's permissions. " +
          "Run `npx jazz-tools validate`: it moves them to jazz.permissions.json, which only your backend reads.",
      );
    }
    return candidate as AppArtifact;
  }
  if (typeof candidate === "object" && candidate !== null && "migrations" in candidate) {
    throw new Error(
      "defineApp was given a migrations/index.ts module as its artifact: since alpha.63 an app ships its jazz.app.json instead. " +
        SHIP_ARTIFACT_HINT,
    );
  }
  if (typeof candidate === "object" && candidate !== null && "default" in candidate) {
    throw new Error(
      "defineApp's `artifact` is a module namespace, not the jazz.app.json it holds: import the file's default export, " +
        '`import artifact from "./jazz.app.json" with { type: "json" };`.',
    );
  }
  throw new Error(
    "defineApp's `artifact` is not a jazz.app.json (it needs `format`, `current` and `schemas`). " +
      SHIP_ARTIFACT_HINT,
  );
}

function define(schema: WasmSchema, key: symbol, value: unknown): void {
  Object.defineProperty(schema, key, {
    value,
    enumerable: false,
    configurable: true,
    writable: false,
  });
}

/** The `jazz.app.json` the app defining `schema` ships, if it ships one. */
export function shippedArtifact(schema: WasmSchema): AppArtifact | undefined {
  return (schema as { [SHIPPED]?: AppArtifact })[SHIPPED];
}

/** Whether the app defining `schema` passed `{ migrations: "none" }` to `defineApp`. */
export function shipsNoMigrations(schema: WasmSchema): boolean {
  return (schema as { [NONE]?: true })[NONE] === true;
}

/** Whether `artifact` ships a migration, so its history is more than its current schema. */
export function shipsMigrations(artifact: AppArtifact | undefined): boolean {
  return (artifact?.migrations?.length ?? 0) > 0;
}
