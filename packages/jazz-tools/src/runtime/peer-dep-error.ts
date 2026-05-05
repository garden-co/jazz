// Error codes that indicate a dynamic import failed because the module is not
// installed. Anything else (init crashes, syntax errors in wasm-bindgen glue,
// native-binding faults) must rethrow so we don't misdiagnose real failures.
const MODULE_NOT_FOUND_CODES: ReadonlySet<string> = new Set([
  "ERR_MODULE_NOT_FOUND",
  "MODULE_NOT_FOUND",
  "ERR_PACKAGE_PATH_NOT_EXPORTED",
]);

export function isModuleNotFoundError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  return typeof code === "string" && MODULE_NOT_FOUND_CODES.has(code);
}
