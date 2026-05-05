import jazzRn from "jazz-rn";
import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type RuntimeTokenOptions,
} from "../runtime/db-runtime-module.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

export interface ReactNativeRuntimeDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

type ReactNativeIdentityBinding = {
  mintLocalFirstToken(seedB64: string, audience: string, ttlSeconds: bigint): string;
  mintAnonymousToken(seedB64: string, audience: string, ttlSeconds: bigint): string;
};

function identityBinding(): ReactNativeIdentityBinding {
  return jazzRn.jazz_rn as ReactNativeIdentityBinding;
}

export class ReactNativeRuntimeModule extends DbRuntimeModule<ReactNativeRuntimeDbConfig> {
  override readonly supportsBrowserWorker = false;
  override readonly supportsPolicyBypass = false;

  protected override async loadRuntime(): Promise<void> {
    return;
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: DbRuntimeClientContext<ReactNativeRuntimeDbConfig>): JazzClient {
    const tier = config.tier ?? "local";
    const runtime = createJazzRnRuntime({
      schema,
      appId: config.appId,
      env: config.env,
      userBranch: config.userBranch,
      tier,
      dataPath: config.dataPath,
    });

    return JazzClient.connectWithRuntime(
      runtime,
      {
        appId: config.appId,
        schema,
        serverUrl: config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        adminSecret: config.adminSecret,
        tier,
        defaultDurabilityTier: "local",
      },
      {
        onAuthFailure,
      },
    );
  }

  override mintLocalFirstToken(options: RuntimeTokenOptions): string {
    return identityBinding().mintLocalFirstToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return identityBinding().mintAnonymousToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }
}
