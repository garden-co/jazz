/**
 * Error for an optional peer dependency that could not be loaded. Only a
 * "module not found" for that very package asks the user to install it; any
 * other failure (an installed but incompatible version, an ESM/CJS mismatch, a
 * missing transitive dependency) says it failed to load and keeps the original
 * error as `cause`.
 */
export function optionalPeerImportError(
  feature: string,
  pkg: string,
  error: unknown,
  hints: { requireEsm?: string } = {},
): Error {
  if (isModuleNotFound(pkg, error)) {
    return new Error(`${feature} needs the ${pkg} package: npm install ${pkg}`, { cause: error });
  }
  const why = error instanceof Error ? error.message : String(error);
  if (isRequireEsm(error)) {
    // Node 22.12+ can require() an ES module, but Vercel's Node runtime
    // turns that off, so a package whose CommonJS files require() an
    // ES-only dependency loads locally and fails only once deployed.
    return new Error(
      `${feature} could not load the ${pkg} package: it require()s an ES module, which this Node ` +
        "process doesn't allow (Vercel's functions run Node with --no-experimental-require-module)." +
        (hints.requireEsm ? ` ${hints.requireEsm}` : "") +
        ` The error: ${why}`,
      { cause: error },
    );
  }
  return new Error(`${feature} could not load the ${pkg} package: ${why}`, { cause: error });
}

function isRequireEsm(error: unknown): boolean {
  return error instanceof Error && (error as { code?: unknown }).code === "ERR_REQUIRE_ESM";
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
