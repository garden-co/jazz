/**
 * The issuer of every user `createJazz` names: `(jazz:backend, userId)`.
 * A core admits only its own app's backend, so the issuer needs no app id.
 */
export const BACKEND_USER_ISSUER = "jazz:backend";
