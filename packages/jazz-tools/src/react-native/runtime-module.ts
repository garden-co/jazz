import type { JazzClient, DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  CoreSource,
  type CoreClientContext,
  type RuntimeTokenOptions,
} from "../runtime/core-source.js";

export interface ReactNativeRuntimeDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export const REACT_NATIVE_CORE_UNSUPPORTED_MESSAGE =
  "[jazz-tools] React Native is unsupported in the core runtime. " +
  "Use a non-React-Native runtime until React Native is ported onto the core runtime.";

export function createReactNativeCoreUnsupportedError(): Error {
  return new Error(REACT_NATIVE_CORE_UNSUPPORTED_MESSAGE);
}

export class ReactNativeCoreSource extends CoreSource<ReactNativeRuntimeDbConfig> {
  override readonly supportsPolicyBypass = false;

  protected override async loadCore(): Promise<void> {
    throw createReactNativeCoreUnsupportedError();
  }

  override createClient(_context: CoreClientContext<ReactNativeRuntimeDbConfig>): JazzClient {
    throw createReactNativeCoreUnsupportedError();
  }

  override mintLocalFirstToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeCoreUnsupportedError();
  }

  override mintAnonymousToken(_options: RuntimeTokenOptions): string {
    throw createReactNativeCoreUnsupportedError();
  }
}
