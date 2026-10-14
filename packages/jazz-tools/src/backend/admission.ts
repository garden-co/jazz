// A hibernated Jazz Cloud app wakes on its first request: the gateway holds or
// refuses that request while the core boots, which can take from a few seconds
// up to a couple of minutes when a node must be provisioned. Admission is a
// read-only secret check, so it is safe to repeat until the core answers.
const WAKE_BUDGET_MS = 120_000;
const ATTEMPT_TIMEOUT_MS = 30_000;
const RETRY_INITIAL_DELAY_MS = 250;
const RETRY_MAX_DELAY_MS = 5_000;
const RETRY_AFTER_MAX_MS = 30_000;
// Statuses a gateway or a still-booting core return before any decision.
const WAKING_STATUSES = new Set([502, 503, 504]);

export interface BackendAdmissionOptions {
  fetch?: typeof fetch;
  wakeBudgetMs?: number;
  attemptTimeoutMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  random?: () => number;
}

/** Capped exponential backoff with equal jitter: [cap/2, cap] of each step. */
function retryDelay(retry: number, random: () => number): number {
  const ceiling = Math.min(RETRY_INITIAL_DELAY_MS * 2 ** Math.min(retry, 16), RETRY_MAX_DELAY_MS);
  return Math.round(ceiling / 2 + random() * (ceiling / 2));
}

/** Retry-After as delta-seconds or an HTTP-date, in milliseconds from `now`. */
export function parseRetryAfter(value: string | null, now: number): number | undefined {
  if (value === null) return undefined;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1_000;
  const date = Date.parse(trimmed);
  return Number.isNaN(date) ? undefined : Math.max(0, date - now);
}

class AttemptTimeout extends Error {}

/**
 * Admit a backend secret with the core at `url`, waiting out an app wake.
 * Gateway/unready statuses (502, 503, 504), network failures and stalled
 * attempts retry with jittered backoff, honouring Retry-After, until the wake
 * budget is spent. Any other status is the core's decision and is final.
 */
export async function admitBackendSecret(
  url: string,
  backendSecret: string,
  options: BackendAdmissionOptions = {},
): Promise<void> {
  const fetcher = options.fetch ?? globalThis.fetch;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? ((ms) => new Promise<void>((done) => setTimeout(done, ms)));
  const random = options.random ?? Math.random;
  const attemptTimeoutMs = options.attemptTimeoutMs ?? ATTEMPT_TIMEOUT_MS;
  const wakeBudgetMs = options.wakeBudgetMs ?? WAKE_BUDGET_MS;
  const deadline = now() + wakeBudgetMs;
  for (let retry = 0; ; retry++) {
    const remaining = Math.max(1, deadline - now());
    let failure: Error;
    let retryAfter: number | undefined;
    try {
      const response = await attempt(
        fetcher,
        url,
        backendSecret,
        Math.min(attemptTimeoutMs, remaining),
      );
      if (response.status === 204) return;
      failure = new Error(`Backend admission failed (${response.status})`);
      if (!WAKING_STATUSES.has(response.status)) throw failure;
      retryAfter = parseRetryAfter(response.headers.get("retry-after"), now());
    } catch (error) {
      if (error instanceof AttemptTimeout) {
        failure = new Error(
          `Backend admission timed out: the app did not answer within ${Math.round(wakeBudgetMs / 1_000)}s`,
        );
      } else if (error instanceof TypeError) {
        // fetch rejects with TypeError for connection-level failures.
        failure = new Error(`Backend admission could not reach the app: ${error.message}`, {
          cause: error,
        });
      } else {
        throw error;
      }
    }
    const delay = Math.min(retryAfter ?? retryDelay(retry, random), RETRY_AFTER_MAX_MS);
    if (now() + delay >= deadline) throw failure;
    await sleep(delay);
  }
}

async function attempt(
  fetcher: typeof fetch,
  url: string,
  backendSecret: string,
  timeoutMs: number,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = new AttemptTimeout();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(timeout);
          controller.abort(timeout);
        }, timeoutMs);
      }),
      (async () => {
        const response = await fetcher(url, {
          method: "POST",
          headers: { "X-Jazz-Backend-Secret": backendSecret },
          // Never follow a redirect with the secret; a 3xx is a final failure.
          redirect: "manual",
          signal: controller.signal,
        });
        // Admission carries no body; release the connection before retrying.
        await response.body?.cancel().catch(() => {});
        return response;
      })(),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
