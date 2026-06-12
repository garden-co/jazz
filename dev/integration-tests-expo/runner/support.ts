// Async polling/timeout helpers, mirrored from the browser test support layer
// (packages/jazz-tools/tests/browser/support.ts). Pure JS — runs on Node and
// Hermes. No RN imports here so it can be unit-tested in Node.

export interface Queryable {
  all<T>(query: unknown): Promise<T[]>;
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

let __seq = 0;
export function uniqueAppId(label: string): string {
  __seq += 1;
  return `itest-${label}-${Date.now().toString(36)}-${__seq.toString(36)}`;
}

export async function waitForCondition(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(50);
  }
  const suffix = lastError ? `; lastError=${errMsg(lastError)}` : "";
  throw new Error(`Timeout after ${timeoutMs}ms: ${message}${suffix}`);
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Timeout after ${timeoutMs}ms: ${label}`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function waitForQuery<T>(
  db: Queryable,
  query: unknown,
  predicate: (rows: T[]) => boolean,
  label: string,
  timeoutMs = 10_000,
): Promise<T[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      lastRows = await db.all<T>(query);
      if (predicate(lastRows)) return lastRows;
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }
  const suffix = lastError ? `; lastError=${errMsg(lastError)}` : "";
  throw new Error(
    `Timeout after ${timeoutMs}ms: ${label}; lastRowsCount=${lastRows.length}${suffix}`,
  );
}

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
