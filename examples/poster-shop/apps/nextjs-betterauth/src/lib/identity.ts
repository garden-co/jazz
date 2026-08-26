/** Canonical issuer-scoped identity stored in PosterShop membership rows. */
export function authorForSession(issuer: string, userId: string): string {
  return JSON.stringify([issuer, userId]);
}

export const configuredIssuer = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export type CanvasMembershipRole = "viewer" | "editor" | "admin";
export type CanvasMembershipIdentity = {
  canvasId: string;
  memberAuthor: string;
  role: CanvasMembershipRole;
};

/** Select a role only from the current author's row on the active canvas. */
export function roleForActiveCanvas(
  memberships: readonly CanvasMembershipIdentity[],
  canvasId: string | undefined,
  author: string | null,
): CanvasMembershipRole | undefined {
  if (!canvasId || !author) return undefined;
  return memberships.find(
    (membership) => membership.canvasId === canvasId && membership.memberAuthor === author,
  )?.role;
}
