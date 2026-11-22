import { createHmac, randomBytes } from "node:crypto";

/**
 * A key this backend signs its own short-lived statements with (link
 * challenges, session renewal tickets), for `label` and app `appId`.
 * Derived from `secret` (the app's secret, or the backend signing key its
 * core hands it), so every instance of the backend derives the same key.
 * Without one, a random key of this instance's own: then only the instance
 * that signed a statement accepts it.
 */
export function serverSigningKey(
  label: string,
  appId: string,
  secret: string | Buffer | undefined,
): Buffer {
  if (secret === undefined) return randomBytes(32);
  return createHmac("sha256", secret).update(`${label}\0${appId}`).digest();
}

/** The core route that hands a trusted backend the app's signing key (jazz#5553). */
export const BACKEND_SIGNING_KEY_ROUTE = "admin/backend-signing-key";

/**
 * The signing keys of a backend, one per label (see {@link serverSigningKey}).
 *
 * With the app's `secret`, they derive from it. Without one (a core reached
 * with a backend credential alone: `vercel()`, `remote()` with
 * `backendCredential`), they derive from the key the core hands its trusted
 * backend, so every instance of the backend shares them: `fetchShared` asks
 * the core for it, once, on the first key asked for (one request in flight;
 * a failed request is asked again on the next use). An older core without
 * the route answers `null`: then each instance keeps keys of its own, as
 * before, and it is said once.
 *
 * The keys stay in this process's memory.
 */
export function backendSigningKeys(options: {
  appId: string;
  secret: string | undefined;
  fetchShared: () => Promise<Buffer | null>;
  warn?: (message: string) => void;
}): (label: string) => Promise<Buffer> {
  const keys = new Map<string, Buffer>();
  // `null`: the core has no shared key, and this instance keeps its own.
  let shared: Buffer | null | undefined = options.secret === undefined ? undefined : null;
  let fetching: Promise<Buffer | null> | undefined;
  const keyOf = (label: string, root: Buffer | null) => {
    let key = keys.get(label);
    if (!key) {
      key = serverSigningKey(label, options.appId, options.secret ?? root ?? undefined);
      keys.set(label, key);
    }
    return key;
  };
  return async (label) => {
    if (keys.has(label)) return keys.get(label)!;
    if (shared === undefined) {
      fetching ??= options.fetchShared().then(
        (fetched) => {
          if (fetched === null) {
            (options.warn ?? console.warn)(
              "jazz: this backend's Jazz core hands out no backend signing key (it predates jazz#5553), so each backend instance signs guest link challenges and session renewal tickets with a key of its own: a guest's sign-up fails with invalid_link_challenge when its requests reach two instances. Upgrade the core to jazz-tools 2.0.0-alpha.68 or later.",
            );
          }
          shared = fetched;
          return fetched;
        },
        (error: unknown) => {
          fetching = undefined;
          throw error;
        },
      );
      await fetching;
    }
    return keyOf(label, shared!);
  };
}
