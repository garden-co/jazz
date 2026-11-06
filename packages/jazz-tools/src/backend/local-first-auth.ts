/** Match the sync-server CLI default while preserving explicit Node overrides. */
export function resolveAllowLocalFirstAuth(allowLocalFirstAuth?: boolean): boolean {
  return allowLocalFirstAuth ?? process.env.NODE_ENV?.toLowerCase() !== "production";
}
