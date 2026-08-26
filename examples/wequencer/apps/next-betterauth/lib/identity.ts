/** Canonical issuer-scoped Jazz author used by provenance and app identity rows. */
export function authorForSession(issuer: string, userId: string): string {
  return JSON.stringify([issuer, userId]);
}
