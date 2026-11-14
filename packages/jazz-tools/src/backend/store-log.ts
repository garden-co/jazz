/**
 * Lines a store logs when its core opens, each said once per process: a
 * Next.js server bundles each route on its own, and a dev server's hot
 * reload opens the same store again, so a module-level flag would repeat it.
 */
const LOGGED = Symbol.for("jazz-tools.storeOpenedLogged");
const logged = globalThis as { [LOGGED]?: Set<string> };

/** Say which store and core this process opened, e.g. `jazz: store sqlite at .jazz/local`. */
export function logStoreOpened(line: string, log: (line: string) => void = console.info): void {
  const said = (logged[LOGGED] ??= new Set());
  if (said.has(line)) return;
  said.add(line);
  log(line);
}
