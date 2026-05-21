// Module customization hooks that make `node:sqlite` look unavailable,
// reproducing the behaviour of Node.js < 22 (and non-Node runtimes) where
// the builtin does not exist.
//
// Used by fallback-no-sqlite.test.ts via block-node-sqlite.register.mjs.

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "node:sqlite" || specifier === "sqlite") {
    throw Object.assign(new Error(`No such built-in module: ${specifier}`), {
      code: "ERR_UNKNOWN_BUILTIN_MODULE",
    });
  }
  return nextResolve(specifier, context);
}
