/** The issuer Better Auth writes into every JWT, and the one Jazz trusts. */
export const configuredIssuer = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
