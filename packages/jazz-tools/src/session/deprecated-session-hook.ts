const warned = new Set<string>();

/**
 * @internal Warn once, outside production builds, that a framework's
 * permission-session hook (`useSession()`, Svelte `getSession()`) is
 * deprecated. It still works in alpha.65; it throws, naming its replacement,
 * from a later release.
 */
export function warnSessionHookDeprecated(name: string, replacement: string): void {
  if (warned.has(name) || isProductionBuild()) return;
  warned.add(name);
  console.warn(
    `[jazz] ${name} is deprecated and will be removed in a later alpha: use ${replacement} ` +
      "(its `user` is what your permissions see as `session`; `user` is now `accountId`).",
  );
}

/**
 * Spelled `process.env.NODE_ENV` so bundlers replace it with the build's mode.
 * Where nothing replaced it and there is no `process` (an unbundled browser
 * module), it throws, and that counts as development.
 */
function isProductionBuild(): boolean {
  try {
    return process.env.NODE_ENV === "production";
  } catch {
    return false;
  }
}
