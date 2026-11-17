/**
 * What `jazz-tools` resolves to inside the app TS that `jazz-tools generate`
 * loads (schema.ts, permissions.ts, migrations/*.ts): the authoring DSL
 * alone, without the runtime, so loading a schema never starts a client,
 * a native addon or WASM.
 */
export { schema } from "./schema-namespace.js";
export { migration } from "./migration-namespace.js";
