import type { AccountHandle } from "./state.js";
import { accountRegistry, AccountAuthError } from "./enrollment.js";

// Context admission is process-local and non-serializable. An accountId copied
// into an ordinary low-level config must not select an account's local storage.
const admitted = new WeakMap<object, string>();

/** @internal Only the handle factory installs account-bound runtime configuration. */
export function admitAccountConfig(config: { accountId?: string }, handle: AccountHandle): void {
  accountRegistry(handle); // Reject forged and logged-out handles.
  if (config.accountId !== handle.id) throw new AccountAuthError("invalid_account_handle");
  admitted.set(config, handle.id);
}

/** @internal Raw runtime entry points must not admit a caller-supplied account UUID. */
export function assertAccountConfig(config: { accountId?: string }): void {
  if (config.accountId !== undefined && admitted.get(config) !== config.accountId) {
    throw new AccountAuthError("account_handle_required");
  }
}

/** @internal Preserve admission across the runtime's own config normalization. */
export function copyAccountConfigAdmission(
  source: { accountId?: string },
  target: { accountId?: string },
): void {
  assertAccountConfig(source);
  if (source.accountId !== undefined) {
    if (target.accountId !== source.accountId)
      throw new AccountAuthError("account_application_mismatch");
    admitted.set(target, source.accountId);
  }
}
