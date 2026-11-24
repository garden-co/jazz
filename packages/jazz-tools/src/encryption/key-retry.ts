type Space = { scopeId: string; identifier: string };
type WriteRetry = "retry" | "last" | "stop";

export function pendingSpace(error: unknown): Space | undefined {
  if (
    !(error instanceof Error) ||
    !("reason" in error) ||
    (error.reason !== "key-pending" && error.reason !== "space-pending") ||
    !("space" in error)
  ) {
    return undefined;
  }
  const space = error.space;
  if (
    !space ||
    typeof space !== "object" ||
    !("scopeId" in space) ||
    typeof space.scopeId !== "string" ||
    !("identifier" in space) ||
    typeof space.identifier !== "string"
  )
    return undefined;
  return { scopeId: space.scopeId, identifier: space.identifier };
}

/** Keep immediate writes synchronous. Rust decides whether and when to repeat a refusal. */
export function retryAfterKey<T>(
  operation: () => T,
  decide: (space: Space, startedAt: number, repeated: boolean) => Promise<WriteRetry>,
): T | Promise<T> {
  try {
    return operation();
  } catch (first) {
    const initial = pendingSpace(first);
    if (!initial) throw first;
    const startedAt = Date.now();
    return (async () => {
      let space = initial;
      let error = first;
      let repeated = false;
      for (;;) {
        const decision = await decide(space, startedAt, repeated);
        if (decision === "last") return operation();
        if (decision !== "retry") throw error;
        try {
          return operation();
        } catch (next) {
          const address = pendingSpace(next);
          if (!address) throw next;
          space = address;
          error = next;
          repeated = true;
        }
      }
    })();
  }
}
