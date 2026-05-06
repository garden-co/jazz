// Error codes that indicate a dynamic import failed because the module is not
// installed. Anything else (init crashes, syntax errors in wasm-bindgen glue,
// native-binding faults) must rethrow so we don't misdiagnose real failures.
const MODULE_NOT_FOUND_CODES: ReadonlySet<string> = new Set([
  "ERR_MODULE_NOT_FOUND",
  "MODULE_NOT_FOUND",
  "ERR_PACKAGE_PATH_NOT_EXPORTED",
]);

// When a `specifier` is provided, also verify the error names that exact
// package — otherwise a transitive failure (e.g. jazz-rn loaded fine but its
// own dependency is missing) would be misreported as the peer dep itself
// being absent. Node emits messages in a few shapes:
//   Cannot find module 'X'
//   Cannot find package 'X' imported from ...
//   Package subpath '...' is not defined by exports in /path/to/X/package.json
export function isModuleNotFoundError(err: unknown, specifier?: string): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  if (typeof code !== "string" || !MODULE_NOT_FOUND_CODES.has(code)) return false;
  if (specifier === undefined) return true;
  const message = (err as { message?: unknown }).message;
  if (typeof message !== "string") return false;
  return (
    message.includes(`'${specifier}'`) ||
    message.includes(`"${specifier}"`) ||
    message.includes(`/${specifier}/package.json`)
  );
}
