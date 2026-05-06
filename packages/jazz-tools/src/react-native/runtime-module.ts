import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type RuntimeTokenOptions,
} from "../runtime/db-runtime-module.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";
import { getJazzRnSync, loadJazzRn } from "./jazz-rn-loader.js";

export interface ReactNativeRuntimeDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export class ReactNativeRuntimeModule extends DbRuntimeModule<ReactNativeRuntimeDbConfig> {
  override readonly supportsBrowserWorker = false;
  override readonly supportsPolicyBypass = false;

  protected override async loadRuntime(): Promise<void> {
    await loadJazzRn();
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
    return getJazzRnSync().jazz_rn.mintLocalFirstToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return getJazzRnSync().jazz_rn.mintAnonymousToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }
}
