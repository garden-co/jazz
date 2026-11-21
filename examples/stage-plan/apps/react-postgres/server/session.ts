// Anonymous sessions, like the Jazz device account: no sign-up. The first
// request gets a random account id in an HMAC-signed, HTTP-only cookie.
import { createHmac, timingSafeEqual } from "node:crypto";

const secret = process.env.AUTH_SECRET ?? "stage-plan-dev-secret";
export const COOKIE = "sid";

const sign = (account: string) => createHmac("sha256", secret).update(account).digest("base64url");

export const sessionValue = (account: string) => `${account}.${sign(account)}`;

/** The account in a signed cookie value, if the signature holds. */
export function accountFromValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const dot = value.lastIndexOf(".");
  const account = value.slice(0, dot);
  const given = Buffer.from(value.slice(dot + 1));
  const expected = Buffer.from(sign(account));
  return dot > 0 && given.length === expected.length && timingSafeEqual(given, expected) ? account : undefined;
}

/** For the WebSocket upgrade, which arrives as a raw Node request. */
export function accountFromCookieHeader(header: string | undefined) {
  const match = header?.match(new RegExp(`(?:^|;\\s*)${COOKIE}=([^;]+)`));
  return accountFromValue(match ? decodeURIComponent(match[1]) : undefined);
}
