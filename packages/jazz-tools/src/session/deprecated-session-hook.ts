const warned = new Set<string>();

/**
 * @internal Warn once, outside production builds, that a framework's
 * permission-session hook (`useSession()`, Svelte `getSession()`) is
 * deprecated. It still works in alpha.65; it throws, naming its replacement,
 * from a later release.
 */
export function warnSessionHookDeprecated(name: string, replacement: string): void {
  if (warned.has(name)) return;
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
    ?.env;
  if (env?.NODE_ENV === "production") return;
  warned.add(name);
  console.warn(
    `[jazz] ${name} is deprecated and will be removed in a later alpha: use ${replacement} ` +
      "(its `user` is what your permissions see as `session`; `user` is now `accountId`).",
  );
}
