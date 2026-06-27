import type { JazzClient, DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  DirectCoreSource,
  type DirectCoreClientContext,
  type RuntimeTokenOptions,
} from "../runtime/direct-core-source.js";

export interface ReactNativeRuntimeDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export const REACT_NATIVE_DIRECT_CORE_ALPHA_UNSUPPORTED_MESSAGE =
  "[jazz-tools] React Native is unsupported in the direct-core alpha runtime. " +
  "Use a non-React-Native runtime until React Native is ported onto the direct-core runtime.";

export function createReactNativeDirectCoreAlphaUnsupportedError(): Error {
  return new Error(REACT_NATIVE_DIRECT_CORE_ALPHA_UNSUPPORTED_MESSAGE);
}

export class ReactNativeCoreSource extends DirectCoreSource<ReactNativeRuntimeDbConfig> {
  override readonly supportsPolicyBypass = false;

  protected override async loadCore(): Promise<void> {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override createClient(_context: DirectCoreClientContext<ReactNativeRuntimeDbConfig>): JazzClient {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }

  override mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeDirectCoreAlphaUnsupportedError();
  }
}
