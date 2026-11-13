/**
 * An ES module that imports a CommonJS one by its default export gets the
 * whole `module.exports`. For a compiled `export default x` (or `export
 * const app = …` read through a default import), that is `{ default: x }`
 * (its `__esModule` flag is not enumerable). It happens to an Expo app, a
 * CommonJS package without `"type": "module"`, whose schema.ts and
 * permissions.ts an ESM backend imports.
 */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * The module `value` wraps, if it is such a wrapper: an object whose only
 * enumerable key is `default`, holding an object. The caller decides whether
 * that object is what it wanted; a table named `default` is a legal name.
 */
export function commonJsWrapped(value: unknown): Record<string, unknown> | undefined {
  if (!isRecord(value)) return undefined;
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== "default") return undefined;
  const inner = value.default;
  return isRecord(inner) ? inner : undefined;
}

/** The error for `file`'s module, arrived as a CommonJS `{ default }` wrapper. */
export function commonJsWrapperError(file: "schema.ts" | "permissions.ts"): Error {
  return new Error(
    `${file} arrived as { default: … }: an ES module imported it as CommonJS, so it got the whole module.exports. ` +
      `This happens when schema.ts and permissions.ts sit in a package without "type": "module" (an Expo app's root) and an ESM backend imports them. ` +
      `Move them into a folder with a package.json of {"type": "module"}, or make the importing package CommonJS too.`,
  );
}
