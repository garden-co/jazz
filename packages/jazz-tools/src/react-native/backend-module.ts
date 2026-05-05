import jazzRn from "jazz-rn";
import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import type { DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import {
  type BackendTokenOptions,
  DbBackendModule,
  type DbBackendClientContext,
} from "../runtime/db-backend.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

export interface ReactNativeBackendDbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export class ReactNativeBackendModule extends DbBackendModule<ReactNativeBackendDbConfig> {
  override readonly supportsBrowserWorker = false;
  override readonly supportsPolicyBypass = false;

  protected override async loadResources(): Promise<void> {
    return;
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: DbBackendClientContext<ReactNativeBackendDbConfig>): JazzClient {
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

  override mintLocalFirstToken(options: BackendTokenOptions): string {
    return jazzRn.jazz_rn.mintLocalFirstToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }

  override mintAnonymousToken(options: BackendTokenOptions): string {
    return jazzRn.jazz_rn.mintAnonymousToken(
      options.secret,
      options.audience,
      BigInt(options.ttlSeconds),
    );
  }
}
