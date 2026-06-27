import type { JazzClient, DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type RuntimeTokenOptions,
} from "../runtime/db-runtime-module.js";

export interface ReactNativeRuntimeDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export const REACT_NATIVE_DIRECT_CORE_ALPHA_UNSUPPORTED_MESSAGE =
  "[jazz-tools] React Native is unsupported in the direct-core alpha runtime. " +
  "Use a non-React-Native runtime for now; the legacy alpha RuntimeCore path is intentionally disabled.";

export function createReactNativeDirectCoreAlphaUnsupportedError(): Error {
  return new Error(REACT_NATIVE_DIRECT_CORE_ALPHA_UNSUPPORTED_MESSAGE);
}

export class ReactNativeRuntimeModule extends DbRuntimeModule<ReactNativeRuntimeDbConfig> {
  override readonly supportsPolicyBypass = false;

  protected override async loadRuntime(): Promise<void> {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override createClient(_context: DbRuntimeClientContext<ReactNativeRuntimeDbConfig>): JazzClient {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }
}
