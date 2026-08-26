/** Canonical issuer-scoped identity stored in PosterShop membership rows. */
export function authorForSession(issuer: string, userId: string): string {
  return JSON.stringify([issuer, userId]);
}

export const configuredIssuer = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
