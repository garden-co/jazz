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
 * Ask `stillAdmitted` every `everyMs`, one check at a time, and call
 * `refused` once (then stop) when it answers false. A check that throws (the
 * app's `authenticate` failed, its database is down) is not a refusal: the
 * next one asks again. Returns a stop.
 */
export function recheckEvery(
  everyMs: number,
  stillAdmitted: () => boolean | Promise<boolean>,
  refused: () => void,
): () => void {
  let checking = false;
  let stopped = false;
  const timer = setInterval(() => {
    if (checking || stopped) return;
    checking = true;
    void Promise.resolve()
      .then(stillAdmitted)
      .then(
        (admitted) => {
          if (stopped || admitted) return;
          stop();
          refused();
        },
        () => {},
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
