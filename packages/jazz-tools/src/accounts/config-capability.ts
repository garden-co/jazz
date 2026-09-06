import type { AccountHandle } from "./state.js";
import { accountRegistry, AccountAuthError } from "./enrollment.js";

// Context admission is process-local and non-serializable. An accountId copied
// into an ordinary low-level config must not select an account's local storage.
type AccountConfigBinding = {
  accountId?: string;
  accountRegistryAuthority?: string;
  runtimeSources?: { browserWorkerPort?: MessagePort };
};
const admitted = new WeakMap<
  object,
  Readonly<{ id: string; registry: string; inspectorPort?: MessagePort }>
>();

/** @internal Only the handle factory installs account-bound runtime configuration. */
export function admitAccountConfig(config: AccountConfigBinding, handle: AccountHandle): void {
  const registry = accountRegistry(handle); // Reject forged and logged-out handles.
  if (config.accountId !== handle.id || config.accountRegistryAuthority !== registry)
    throw new AccountAuthError("invalid_account_handle");
  admitted.set(config, { id: handle.id, registry });
}

/** @internal Raw runtime entry points must not admit a caller-supplied account UUID. */
export function assertAccountConfig(config: AccountConfigBinding): void {
  const binding = admitted.get(config);
  if (
    (binding !== undefined ||
      config.accountId !== undefined ||
      config.accountRegistryAuthority !== undefined) &&
    (!binding ||
      binding.id !== config.accountId ||
      binding.registry !== config.accountRegistryAuthority ||
      (binding.inspectorPort !== undefined &&
        binding.inspectorPort !== config.runtimeSources?.browserWorkerPort))
  ) {
    throw new AccountAuthError("account_handle_required");
  }
}

/** @internal Preserve admission across the runtime's own config normalization. */
export function copyAccountConfigAdmission(
  source: AccountConfigBinding,
  target: AccountConfigBinding,
): void {
  assertAccountConfig(source);
  if (source.accountId !== undefined) {
    if (
      target.accountId !== source.accountId ||
      target.accountRegistryAuthority !== source.accountRegistryAuthority ||
      (admitted.get(source)?.inspectorPort !== undefined &&
        target.runtimeSources?.browserWorkerPort !== admitted.get(source)?.inspectorPort)
    )
      throw new AccountAuthError("account_application_mismatch");
    admitted.set(target, admitted.get(source)!);
  }
}

/** @internal Only the diagnostic factory calls this after worker scope verification.
 * This grant cannot be converted into an ordinary context by dropping its port.
 */
export function admitInspectorAccountConfig(config: AccountConfigBinding): void {
  const inspectorPort = config.runtimeSources?.browserWorkerPort;
  if (!config.accountId || !config.accountRegistryAuthority || !inspectorPort)
    throw new AccountAuthError("account_handle_required");
  admitted.set(config, {
    id: config.accountId,
    registry: config.accountRegistryAuthority,
    inspectorPort,
  });
}
