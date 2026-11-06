/**
 * Error for an optional peer dependency that could not be loaded. Only a
 * "module not found" for that very package asks the user to install it; any
 * other failure (an installed but incompatible version, an ESM/CJS mismatch, a
 * missing transitive dependency) says it failed to load and keeps the original
 * error as `cause`.
 */
export function optionalPeerImportError(feature: string, pkg: string, error: unknown): Error {
  if (isModuleNotFound(pkg, error)) {
    return new Error(`${feature} needs the ${pkg} package: npm install ${pkg}`, { cause: error });
  }
  const why = error instanceof Error ? error.message : String(error);
  return new Error(`${feature} could not load the ${pkg} package: ${why}`, { cause: error });
}

function isModuleNotFound(pkg: string, error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = (error as { code?: unknown }).code;
  const notFound =
    code === "ERR_MODULE_NOT_FOUND" ||
    code === "MODULE_NOT_FOUND" ||
    /Cannot find (module|package)/.test(error.message);
  return notFound && error.message.includes(`'${pkg}'`);
}
