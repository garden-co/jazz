import type { JazzUser } from "./create-jazz.js";

/** Where the docs explain `expiresAt` and re-authentication. */
export const SESSION_EXPIRY_DOCS_URL =
  "https://jazz.tools/docs/setup/auth/backend#when-a-session-expires";

/**
 * A sync session's `authenticate` answer is good until `expiresAt`; shortly
 * before then the backend asks the client to connect again, and the new
 * connection's request (with the client's current cookies or token) decides
 * again. How long before: this much, or half the session for a short one.
 */
export const REAUTHENTICATE_LEAD_MS = 30_000;

/**
 * The shortest time a session stays open before it is asked to
 * re-authenticate, so an `expiresAt` very close to now can't make a client
 * reconnect in a tight loop.
 */
export const MIN_SESSION_MS = 5_000;

/**
 * `authenticate` named a user without a valid `expiresAt`. Every answer that
 * names a user must say until when it holds: the app's own session expiry
 * (or its token's `exp`), in seconds since the epoch.
 */
export class AuthenticateExpiresAtError extends Error {
  constructor(detail: string) {
    super(
      `createJazz: authenticate must return expiresAt with the user: when this answer stops holding, as Unix time in seconds (your session's expiry, or its token's exp). ${detail} See ${SESSION_EXPIRY_DOCS_URL}`,
    );
    this.name = "AuthenticateExpiresAtError";
  }
}

/** Seconds since the epoch past this are milliseconds (the year 5138). */
const LATEST_PLAUSIBLE_SECONDS = 1e11;

/**
 * `answer`, a user `authenticate` returned, checked for its `expiresAt`.
 * Refuses a missing, non-finite or millisecond value with
 * {@link AuthenticateExpiresAtError}, rather than guessing.
 */
export function withCheckedExpiry(answer: JazzUser): JazzUser & { expiresAt: number } {
  const { expiresAt } = answer as { expiresAt?: unknown };
  if (expiresAt === undefined || expiresAt === null)
    throw new AuthenticateExpiresAtError("It returned none.");
  if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt))
    throw new AuthenticateExpiresAtError(`It returned ${String(expiresAt)}.`);
  if (expiresAt > LATEST_PLAUSIBLE_SECONDS)
    throw new AuthenticateExpiresAtError(
      `It returned ${expiresAt}, which looks like milliseconds: divide by 1000.`,
    );
  return answer as JazzUser & { expiresAt: number };
}

/**
 * When a session admitted at `admittedAtMs` and good until `expiresAtMs`
 * is asked to re-authenticate, in milliseconds since the epoch, or null when
 * the answer has expired already (the session must not open).
 */
export function reauthenticateAt(admittedAtMs: number, expiresAtMs: number): number | null {
  const remaining = expiresAtMs - admittedAtMs;
  if (remaining <= 0) return null;
  const lead = Math.min(REAUTHENTICATE_LEAD_MS, remaining / 2);
  return Math.max(admittedAtMs + Math.min(MIN_SESSION_MS, remaining), expiresAtMs - lead);
}

/**
 * The longest delay a timer takes as asked: Node and browsers clamp larger
 * ones (past ~24.8 days) to almost nothing, which would fire a long
 * session's re-authentication right after it opens.
 */
export const MAX_TIMER_DELAY_MS = 2 ** 31 - 1;

/**
 * Call `run` after `delayMs`, however long, chaining timers below
 * {@link MAX_TIMER_DELAY_MS}. Unref'd, so it never keeps a process alive.
 * Returns a cancel.
 */
export function startTimer(run: () => void, delayMs: number): () => void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let cancelled = false;
  const due = Date.now() + Math.max(delayMs, 0);
  const arm = () => {
    const left = due - Date.now();
    timer = setTimeout(
      () => {
        if (cancelled) return;
        if (due - Date.now() > 0) arm();
        else run();
      },
      Math.min(Math.max(left, 0), MAX_TIMER_DELAY_MS),
    );
    (timer as unknown as { unref?: () => void }).unref?.();
  };
  arm();
  return () => {
    cancelled = true;
    if (timer !== undefined) clearTimeout(timer);
  };
}
