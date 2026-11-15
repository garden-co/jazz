/**
 * `jazz-tools/permissions`, the TypeScript permissions DSL, was removed in
 * alpha.65: an app's permissions are its `jazz.permissions.json`. The names
 * stay for one release as stubs that throw, pointing at `jazz-tools upgrade`.
 */
import { removedSchemaApi } from "../removed-schema-api.js";
import type { RemovedApi } from "../removed-api.js";

export { definePermissions, anyOf, allOf } from "../removed-schema-api.js";
/** @deprecated Removed in alpha.65, and throws when used: permissions are written in jazz.permissions.json. Run `npx jazz-tools upgrade`. */
export const createSessionContext: RemovedApi =
  /*#__PURE__*/ removedSchemaApi("createSessionContext");
