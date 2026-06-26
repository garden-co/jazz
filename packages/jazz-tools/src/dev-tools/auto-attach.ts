// Tracks dbs already attached to the inspector devtools bridge, so a provider's
// dev auto-attach and a manual attachDevTools() call don't double-attach the
// same db. Shared across the framework providers (one WeakSet instead of four).
//
// The NODE_ENV gate and the dynamic import("../dev-tools/dev-tools.js") stay
// inline at each provider call site on purpose: that is what lets the consumer's
// bundler drop attachDevTools (and the rest of dev-tools) from production builds.
const attachedDbs = new WeakSet<object>();

/** Returns true the first time it sees a db; false on subsequent calls. */
export function markDevToolsAttached(db: object): boolean {
  if (attachedDbs.has(db)) return false;
  attachedDbs.add(db);
  return true;
}
