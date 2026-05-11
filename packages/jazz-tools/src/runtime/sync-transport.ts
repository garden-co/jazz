/**
 * Shared sync transport utilities.
 *
 * Provides auth header helpers used by callers that still build raw HTTP
 * requests. All worker/server transport now lives in Rust; the outbox router
 * abstraction was removed alongside the TS worker-protocol layer.
 */

export type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

/**
 * Apply end-user auth headers. Sets `Authorization: Bearer <token>` when a JWT is available.
 */
export function applyUserAuthHeaders(
  headers: Record<string, string>,
  auth: { jwtToken?: string },
): void {
  if (auth.jwtToken) {
    headers["Authorization"] = `Bearer ${auth.jwtToken}`;
  }
}
