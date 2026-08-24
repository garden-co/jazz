import type { DbConfig } from "./db.js";

function canonicalizeConfigValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeConfigValue);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }

  const canonical: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    canonical[key] = canonicalizeConfigValue((value as Record<string, unknown>)[key]);
  }
  return canonical;
}

/** Stable structural identity for registry and framework config comparisons. */
export function serializeClientConfig(config: DbConfig): string {
  return JSON.stringify(canonicalizeConfigValue(config));
}
