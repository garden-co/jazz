import * as React from "react";

export type Advice = "allowed" | "denied" | "unknown";

/**
 * Asks Jazz whether actions would be allowed (`db.canInsert`, `canUpdate`,
 * `canDelete`) instead of re-implementing the rules from permissions.ts.
 * Pass one check per key; results are cached until `revision` changes, which
 * callers bump whenever folders or memberships change.
 *
 * Advice is a hint for what to show. The sync server still decides every
 * write, and a denied one surfaces through `db.onMutationError`.
 */
export function useAdvice(
  checks: Readonly<Record<string, () => Promise<Advice>>>,
  revision: string,
): Readonly<Record<string, Advice | undefined>> {
  const [results, setResults] = React.useState<Record<string, Advice>>({});
  const cache = React.useRef({ revision, pending: new Set<string>() });
  const keys = Object.keys(checks).sort().join("\n");

  React.useEffect(() => {
    if (cache.current.revision !== revision) {
      cache.current = { revision, pending: new Set() };
      setResults({});
    }
    const current = cache.current;
    for (const [key, check] of Object.entries(checks)) {
      if (current.pending.has(key)) continue;
      current.pending.add(key);
      check().then(
        (advice) => {
          if (cache.current === current) setResults((prev) => ({ ...prev, [key]: advice }));
        },
        () => {
          if (cache.current === current) setResults((prev) => ({ ...prev, [key]: "unknown" }));
        },
      );
    }
    // `checks` is rebuilt every render; the keys and revision identify it.
  }, [keys, revision]);

  return cache.current.revision === revision ? results : {};
}

/** Offer an action unless Jazz said no. While a check is pending, hold it back. */
export function offered(advice: Advice | undefined): boolean {
  return advice === "allowed" || advice === "unknown";
}
