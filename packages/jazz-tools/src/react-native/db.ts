import jazzRn from "jazz-rn";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import { Db as RuntimeDb, type DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

export interface DbConfig extends RuntimeDbConfig {
  dataPath?: string;
  tier?: DurabilityTier;
}

export class Db extends RuntimeDb {
  private readonly nativeClients = new Map<string, JazzClient>();

  constructor(private readonly nativeConfig: DbConfig) {
    // RN uses a native runtime instead of the browser WASM module path.
    super(nativeConfig, null);
  }

  protected override getClient(schema: WasmSchema): JazzClient {
    const key = JSON.stringify(schema);

    if (!this.nativeClients.has(key)) {
      const tier = this.nativeConfig.tier ?? "worker";
      const runtime = createJazzRnRuntime({
        schema,
        appId: this.nativeConfig.appId,
        env: this.nativeConfig.env,
        userBranch: this.nativeConfig.userBranch,
        tier,
        dataPath: this.nativeConfig.dataPath,
      });

      const client = JazzClient.connectWithRuntime(
        runtime,
        {
          appId: this.nativeConfig.appId,
          schema,
          serverUrl: this.nativeConfig.serverUrl,
          serverPathPrefix: this.nativeConfig.serverPathPrefix,
          env: this.nativeConfig.env,
          userBranch: this.nativeConfig.userBranch,
          jwtToken: this.nativeConfig.jwtToken,
          adminSecret: this.nativeConfig.adminSecret,
          tier,
          defaultDurabilityTier: "worker",
        },
        {
          onAuthFailure: (reason) => {
            this.markUnauthenticated(reason);
          },
        },
      );

      if (this.nativeConfig.serverUrl) {
        client.connectTransport(this.nativeConfig.serverUrl, {
          jwt_token: this.nativeConfig.jwtToken,
          admin_secret: this.nativeConfig.adminSecret,
        });
      }

      this.nativeClients.set(key, client);
    }

    return this.nativeClients.get(key)!;
  }

  override updateAuthToken(jwtToken: string | null): void {
    if (!this.applyAuthUpdate(jwtToken)) {
      return;
    }

    for (const client of this.nativeClients.values()) {
      client.updateAuthToken(jwtToken ?? undefined);
    }
  }

  override async shutdown(): Promise<void> {
    for (const client of this.nativeClients.values()) {
      await client.shutdown();
    }
    this.nativeClients.clear();
  }
}

export async function createDb(config: DbConfig): Promise<Db> {
  if (config.auth) {
    const jwtToken = jazzRn.jazz_rn.mintLocalFirstToken(
      config.auth.localFirstSecret,
      config.appId,
      BigInt(3600),
    );
    return new Db({ ...config, jwtToken });
  }
  return new Db(config);
}
