import type { WasmSchema } from "../drivers/types.js";
import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import { Db as RuntimeDb, type DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";

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

      const client = JazzClient.connectWithRuntime(runtime, {
        appId: this.nativeConfig.appId,
        schema,
        serverUrl: this.nativeConfig.serverUrl,
        serverPathPrefix: this.nativeConfig.serverPathPrefix,
        env: this.nativeConfig.env,
        userBranch: this.nativeConfig.userBranch,
        jwtToken: this.nativeConfig.jwtToken,
        localAuthMode: this.nativeConfig.localAuthMode,
        localAuthToken: this.nativeConfig.localAuthToken,
        adminSecret: this.nativeConfig.adminSecret,
        tier,
        defaultDurabilityTier: tier,
      });

      this.nativeClients.set(key, client);
    }

    return this.nativeClients.get(key)!;
  }

  override async shutdown(): Promise<void> {
    for (const client of this.nativeClients.values()) {
      await client.shutdown();
    }
    this.nativeClients.clear();
  }
}

export async function createDb(config: DbConfig): Promise<Db> {
  return new Db(config);
}
