import { ApiError } from "../data/api";

/** The code of a write the server refused, such as "permission_denied", or undefined. */
export function writeErrorCode(error: unknown): string | undefined {
  return error instanceof ApiError ? error.code : undefined;
}

/** Whether the server refused a write because the role doesn't allow it. */
export function isPermissionDenied(error: unknown) {
  return writeErrorCode(error) === "permission_denied";
}
