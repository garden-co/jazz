import { createHmac, randomBytes } from "node:crypto";

/**
 * A key this backend signs its own short-lived statements with (link
 * challenges), for `label` and app `appId`. Derived from `secret`, the
 * app's secret, so every instance of the backend derives the same key.
 * Without one (a store reached with a backend credential alone, which
 * holds only a public stand-in), a random key of this instance's own:
 * then only the instance that signed a statement accepts it.
 */
export function serverSigningKey(label: string, appId: string, secret: string | undefined): Buffer {
  if (secret === undefined) return randomBytes(32);
  return createHmac("sha256", secret).update(`${label}\0${appId}`).digest();
}
