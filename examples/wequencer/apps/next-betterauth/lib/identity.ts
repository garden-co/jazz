/**
 * Construct a canonical author only at this trusted backend boundary, where an
 * external issuer and subject must be mapped to an app identity row. Browser
 * code reads the already-derived `session.user` from Jazz instead.
 */
export { userIdentity as sessionAuthor } from "jazz-tools";
