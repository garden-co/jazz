export interface NormalizedWorkerInitError {
  name: string;
  message: string;
}

export interface OpfsInitRetryPolicy {
  totalTimeoutMs: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterMs: number;
}

export const DEFAULT_OPFS_INIT_RETRY_POLICY: OpfsInitRetryPolicy = {
  totalTimeoutMs: 10_000,
  baseDelayMs: 120,
  maxDelayMs: 1_200,
  jitterMs: 120,
};

export class OpfsInitRetryFailure extends Error {
  readonly attempts: number;
  readonly elapsedMs: number;
  readonly retryable: boolean;
  readonly timedOut: boolean;
  readonly causeError: NormalizedWorkerInitError;

  constructor(args: {
    attempts: number;
    elapsedMs: number;
    retryable: boolean;
    timedOut: boolean;
    causeError: NormalizedWorkerInitError;
  }) {
    const reason = args.timedOut ? "retry timeout" : "non-retryable";
    super(
      `OPFS init ${reason} after ${args.attempts} attempt(s) in ${args.elapsedMs}ms: ${args.causeError.name}: ${args.causeError.message}`,
    );
    this.name = "OpfsInitRetryFailure";
    this.attempts = args.attempts;
    this.elapsedMs = args.elapsedMs;
    this.retryable = args.retryable;
    this.timedOut = args.timedOut;
    this.causeError = args.causeError;
  }
}

export class OpfsInitRetryCancelled extends Error {
  constructor() {
    super("OPFS init retry cancelled");
    this.name = "OpfsInitRetryCancelled";
  }
}

export interface OpenPersistentWithRetryOptions<T> {
  open: () => Promise<T>;
  policy?: OpfsInitRetryPolicy;
  isRetryable?: (error: unknown) => boolean;
  isCancelled?: () => boolean;
  now?: () => number;
  random?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export interface OpenPersistentWithRetryResult<T> {
  value: T;
  attempts: number;
  elapsedMs: number;
}

export function normalizeUnknownWorkerInitError(error: unknown): NormalizedWorkerInitError {
  if (error instanceof Error) {
    return {
      name: error.name || "Error",
      message: error.message || String(error),
    };
  }
  if (typeof error === "string") {
    return {
      name: "Error",
      message: error,
    };
  }
  return {
    name: "UnknownError",
    message: String(error),
  };
}

export function isRetryableOpfsInitError(error: unknown): boolean {
  const normalized = normalizeUnknownWorkerInitError(error);
  const haystack = `${normalized.name} ${normalized.message}`.toLowerCase();

  if (haystack.includes("nomodificationallowederror")) {
    return true;
  }

  const mentionsSyncHandle = haystack.includes("createsyncaccesshandle");
  const mentionsOpenHandleConflict =
    haystack.includes("another open access handle") ||
    haystack.includes("another open access-handle") ||
    (haystack.includes("access handle") && haystack.includes("another open"));

  return mentionsSyncHandle && mentionsOpenHandleConflict;
}

export function computeRetryDelayMs(
  attempt: number,
  policy: OpfsInitRetryPolicy,
  random: () => number = Math.random,
): number {
  const exponent = Math.max(0, attempt - 1);
  const expDelay = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** exponent);
  const jitter = Math.floor(random() * (policy.jitterMs + 1));
  return expDelay + jitter;
}

export async function openPersistentWithRetry<T>(
  options: OpenPersistentWithRetryOptions<T>,
): Promise<OpenPersistentWithRetryResult<T>> {
  const policy = options.policy ?? DEFAULT_OPFS_INIT_RETRY_POLICY;
  const isRetryable = options.isRetryable ?? isRetryableOpfsInitError;
  const isCancelled = options.isCancelled ?? (() => false);
  const now = options.now ?? (() => Date.now());
  const random = options.random ?? (() => Math.random());
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));

  const startedAt = now();
  const deadline = startedAt + policy.totalTimeoutMs;
  let attempts = 0;

  while (true) {
    if (isCancelled()) {
      throw new OpfsInitRetryCancelled();
    }

    attempts += 1;

    try {
      const value = await options.open();
      return {
        value,
        attempts,
        elapsedMs: Math.max(0, now() - startedAt),
      };
    } catch (error) {
      if (isCancelled()) {
        throw new OpfsInitRetryCancelled();
      }

      const retryable = isRetryable(error);
      const elapsedMs = Math.max(0, now() - startedAt);
      if (!retryable) {
        throw new OpfsInitRetryFailure({
          attempts,
          elapsedMs,
          retryable: false,
          timedOut: false,
          causeError: normalizeUnknownWorkerInitError(error),
        });
      }

      const remainingMs = deadline - now();
      if (remainingMs <= 0) {
        throw new OpfsInitRetryFailure({
          attempts,
          elapsedMs,
          retryable: true,
          timedOut: true,
          causeError: normalizeUnknownWorkerInitError(error),
        });
      }

      const delayMs = Math.min(remainingMs, computeRetryDelayMs(attempts, policy, random));
      await sleep(delayMs);
    }
  }
}
