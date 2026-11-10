import type { JazzUser } from "./create-jazz.js";

/**
 * Whether `current`, what `authenticate` says now, is still the user a sync
 * session was admitted as: the same user id with the same claims. The core
 * holds the claims a session was admitted with until it ends, so a session
 * whose user's claims changed must be admitted again for policies to see
 * them. Claims compare as the JSON the core receives, key order aside;
 * claims that don't serialize never match.
 */
export function isSameAdmission(admitted: JazzUser, current: JazzUser | null): boolean {
  if (!current || current.userId !== admitted.userId) return false;
  try {
    return canonicalClaims(current.claims) === canonicalClaims(admitted.claims);
  } catch {
    return false;
  }
}

/** `claims` (none counts as `{}`) as JSON with every object's keys sorted. */
function canonicalClaims(claims: Record<string, unknown> | undefined): string {
  return JSON.stringify(claims ?? {}, (_key, value: unknown) =>
    value && typeof value === "object" && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : value,
  );
}

/**
 * How many re-checks in a row may fail (throw) before a session ends as if
 * `authenticate` had refused it. One failure is a blip (the auth database
 * was down); this many in a row means `authenticate` can't answer for an
 * open session, and a session no one can re-check must not stay open past a
 * sign-out.
 */
export const RECHECK_FAILURES_BEFORE_END = 3;

/** How a re-check treats checks that fail (throw) instead of answering. */
export interface RecheckFailures {
  /** Failed checks in a row that end the session. Defaults to {@link RECHECK_FAILURES_BEFORE_END}. */
  endAfter?: number;
  /**
   * Whether a failure ends the session at once: one that fails the same way
   * on every check (an `authenticate` that can't run outside a request), so
   * waiting for more only delays a sign-out.
   */
  endsAtOnce?: (error: unknown) => boolean;
  /** Called for each failed check, with how many failed in a row so far (from 1). */
  failed?: (error: unknown, inARow: number) => void;
}

/**
 * Why a re-check ended a session: the check answered no, or checks failed
 * (`error` is the last one's).
 */
export type RecheckEnd = { kind: "refused" } | { kind: "failed"; error: unknown; inARow: number };

/**
 * Ask `stillAdmitted` every `everyMs`, one check at a time, and call `ended`
 * once (then stop) when it answers false. A check that throws (the app's
 * `authenticate` failed, its database is down) is not a refusal on its own:
 * the next one asks again. But `failures.endAfter` of them in a row (default
 * {@link RECHECK_FAILURES_BEFORE_END}) end the session, so a check that
 * always throws fails closed, and one `failures.endsAtOnce` picks out ends it
 * at once. An answer resets the count. Returns a stop.
 */
export function recheckEvery(
  everyMs: number,
  stillAdmitted: () => boolean | Promise<boolean>,
  ended: (end: RecheckEnd) => void,
  failures: RecheckFailures = {},
): () => void {
  const endAfter = failures.endAfter ?? RECHECK_FAILURES_BEFORE_END;
  if (!Number.isInteger(endAfter) || endAfter < 1)
    throw new Error(`recheckEvery: failures.endAfter must be a positive integer, got ${endAfter}`);
  let checking = false;
  let stopped = false;
  let failedInARow = 0;
  const end = (why: RecheckEnd) => {
    stop();
    ended(why);
  };
  // Reporting a failure, or judging it, must not keep a session open.
  const quietly = <T>(f: () => T, fallback: T): T => {
    try {
      return f();
    } catch {
      return fallback;
    }
  };
  const timer = setInterval(() => {
    if (checking || stopped) return;
    checking = true;
    void Promise.resolve()
      .then(stillAdmitted)
      .then(
        (admitted) => {
          if (stopped) return;
          failedInARow = 0;
          if (!admitted) end({ kind: "refused" });
        },
        (error: unknown) => {
          if (stopped) return;
          failedInARow += 1;
          quietly(() => failures.failed?.(error, failedInARow), undefined);
          const atOnce = quietly(() => failures.endsAtOnce?.(error) ?? false, true);
          if (atOnce || failedInARow >= endAfter)
            end({ kind: "failed", error, inARow: failedInARow });
        },
      )
      .finally(() => {
        checking = false;
      });
  }, everyMs);
  (timer as unknown as { unref?: () => void }).unref?.();
  const stop = () => {
    stopped = true;
    clearInterval(timer);
  };
  return stop;
}

/**
 * Whether `error` is a framework's "called outside a request scope" error:
 * what Next.js's `cookies()` and `headers()` throw (and the auth helpers
 * built on them: AuthKit's `withAuth()`, NextAuth's `auth()`, Clerk's
 * `auth()`) when called from a timer rather than while handling a request.
 */
export function isOutsideRequestScope(error: unknown): boolean {
  const message = error instanceof Error ? error.message : typeof error === "string" ? error : "";
  return /outside a request scope/i.test(message);
}

/** Where the docs explain what `authenticate` may rely on when a session is re-checked. */
export const RECHECK_DOCS_URL =
  "https://jazz.tools/docs/setup/auth/backend#re-checking-open-connections";

/**
 * What a failed re-check means, said once per process: what a re-check is,
 * what `authenticate` can rely on there, and what Jazz does about failures.
 */
export function recheckFailedWarning(error: unknown): string {
  const scope = isOutsideRequestScope(error)
    ? " It threw a request-scope error: authenticate uses a helper that only works while a request is being handled (in Next.js: cookies(), headers(), or auth helpers built on them, such as AuthKit's withAuth(), NextAuth's auth() or Clerk's auth()). Read the session from request.headers instead."
    : "";
  return (
    "createJazz: authenticate threw while re-checking an open sync session." +
    scope +
    " Jazz calls authenticate(request) again every recheckSeconds, from a timer outside your framework's request scope, with the request that opened the session (and its cookies from then), so it must decide from `request` alone." +
    ` A request-scope error ends the session at once; other errors end it after ${RECHECK_FAILURES_BEFORE_END} failed re-checks in a row. The client then reconnects, as when authenticate no longer names the user.` +
    ` See ${RECHECK_DOCS_URL}`
  );
}
