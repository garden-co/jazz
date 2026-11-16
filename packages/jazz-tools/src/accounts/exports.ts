// Account exports shared by the package root and every framework entry point.
// Accounts themselves are the session's (see `JazzSession`): only the error
// its operations reject with, and the storage a host may supply, are public.
export { AccountAuthError } from "./enrollment.js";
export type { AccountStore } from "./persistence.js";
