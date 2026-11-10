/**
 * The failed write's error class and code. Its message is included only for
 * the engine's own TypeError and ReferenceError, whose messages name code
 * ("undefined is not a function"), never data, with the names of the
 * functions it was thrown in, squeezed to identifier characters, so nothing
 * the app or Core wrote crosses into CI output.
 */
export function publicClientWriteDetail(error: unknown): string {
  const record = (error ?? {}) as { name?: unknown; code?: unknown; message?: unknown };
  const name =
    typeof record.name === "string" && /^[A-Za-z][A-Za-z0-9]{0,47}$/.test(record.name)
      ? record.name
      : "unknown";
  const code =
    typeof record.code === "string" && /^[a-z0-9_]{1,40}$/.test(record.code) ? record.code : "none";
  const engineError = error instanceof TypeError || error instanceof ReferenceError;
  const text =
    engineError && typeof record.message === "string"
      ? [record.message, ...callers((error as Error).stack)].join(" at ")
      : "";
  const message = text.replace(/[^A-Za-z0-9_.]+/g, "_").slice(0, 96);
  return `public-client-write-detail:${name}-${code}${message ? `:${message}` : ""}`;
}

/** The innermost function names of a Hermes (`at f (…)`) or JSC (`f@…`) stack. */
function callers(stack: string | undefined): string[] {
  const names: string[] = [];
  for (const line of (stack ?? "").split("\n")) {
    const name =
      /^\s*at ([A-Za-z0-9_$.]+) /.exec(line)?.[1] ?? /^([A-Za-z0-9_$.]+)@/.exec(line)?.[1];
    if (name) names.push(name);
    if (names.length === 3) break;
  }
  return names;
}
