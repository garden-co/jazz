import { PostcardReader, PostcardWriter } from "./native-codec.js";

/**
 * Renewing a browser's sync session in place, without reconnecting.
 *
 * A browser whose sync session rides on the app's cookie asks for renewal
 * in its prelude ({@link SESSION_RENEWAL_PRELUDE_FIELD}). The app's backend
 * (`createJazz`) then tells it, over the open socket, when to renew
 * ({@link WIRE_FRAME_RENEW_SOON}). At that time the browser makes one
 * ordinary request to the backend (the "who is signed in" route, with
 * {@link SESSION_RENEWAL_HEADER}), where the app's `refresh` middleware may
 * rotate its cookie and `authenticate` decides on a fresh request. The
 * answer carries a short-lived signed ticket, which the browser sends back
 * over the socket ({@link WIRE_FRAME_RENEW}); the backend then extends the
 * session instead of asking the browser to reconnect.
 *
 * Both frames live only between the browser's carrier and the backend's
 * relay: neither ever reaches a core. Anything that goes wrong on the way
 * (the request fails, the ticket is refused) leaves the session to end at
 * its expiry as before, with a reconnect.
 */

/** The prelude field (top level) a client sets to ask for in-place renewal. */
export const SESSION_RENEWAL_PRELUDE_FIELD = "jazz_renewal";

/** The request header naming the session a renewal request is for. */
export const SESSION_RENEWAL_HEADER = "X-Jazz-Renew";

/** Server to client: "renew within `renewInMs`", naming the session. */
export const WIRE_FRAME_RENEW_SOON = 0x40;
/** Client to server: a renewal ticket for this session. */
export const WIRE_FRAME_RENEW = 0x41;

/** The longest a session id or ticket may be, so neither side parses junk. */
const MAX_RENEWAL_STRING = 2048;

export function encodeRenewSoonFrame(session: string, renewInMs: number): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(WIRE_FRAME_RENEW_SOON);
  writer.string(session);
  writer.u64(Math.max(0, Math.floor(renewInMs)));
  return writer.finish();
}

export function decodeRenewSoonFrame(frame: Uint8Array): { session: string; renewInMs: number } {
  const reader = new PostcardReader(frame);
  if (reader.u64() !== WIRE_FRAME_RENEW_SOON) throw new Error("not a renew-soon frame");
  const session = reader.string();
  const renewInMs = reader.u64();
  if (!reader.done() || !session || session.length > MAX_RENEWAL_STRING) {
    throw new Error("malformed renew-soon frame");
  }
  return { session, renewInMs };
}

export function encodeRenewFrame(ticket: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(WIRE_FRAME_RENEW);
  writer.string(ticket);
  return writer.finish();
}

/** The ticket in a renew frame, or null for any other frame. */
export function decodeRenewFrame(frame: Uint8Array): string | null {
  if (frame[0] !== WIRE_FRAME_RENEW) return null;
  try {
    const reader = new PostcardReader(frame);
    reader.u64();
    const ticket = reader.string();
    if (!reader.done() || ticket.length > MAX_RENEWAL_STRING) return null;
    return ticket;
  } catch {
    return null;
  }
}

export function isRenewSoonFrame(frame: Uint8Array): boolean {
  return frame[0] === WIRE_FRAME_RENEW_SOON;
}

/**
 * The backend's "who is signed in" route for a sync socket at `socketUrl`
 * (`…/apps/<app>/ws`), as an HTTP URL.
 */
export function renewalUrlFor(socketUrl: string): string | null {
  try {
    const url = new URL(socketUrl);
    if (!/\/ws\/?$/.test(url.pathname)) return null;
    url.protocol =
      url.protocol === "wss:" ? "https:" : url.protocol === "ws:" ? "http:" : url.protocol;
    url.pathname = url.pathname.replace(/\/ws\/?$/, "/accounts/backend-user");
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

/**
 * The Web Lock every request that may refresh the app's session takes, in
 * every window and worker of a browser: in-place renewals and the session's
 * "who is signed in" checks alike. Per origin, as the cookie it refreshes is.
 */
export function sessionRefreshLockName(url: string): string {
  try {
    return `jazz-session-refresh:${new URL(url.replace(/^ws/, "http")).origin}`;
  } catch {
    return `jazz-session-refresh:${url}`;
  }
}
