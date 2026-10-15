// Account exports shared by the package root and every framework entry point.
export type { AccountHandle, AccountIdentity, AccountSnapshot, AccountManager } from "./state.js";
export {
  AccountAuthError,
  exportLocalFirstSecret,
  type BackendAuth,
  type JWTAuth,
} from "./enrollment.js";
export type { AccountStore } from "./persistence.js";
