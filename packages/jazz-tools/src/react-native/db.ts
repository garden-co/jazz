import jazzRn from "jazz-rn";
import { Platform } from "react-native";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient, type DurabilityTier } from "../runtime/client.js";
import { Db as RuntimeDb, type DbConfig as RuntimeDbConfig } from "../runtime/db.js";
import { createJazzRnRuntime } from "./create-jazz-rn-runtime.js";

/**
 * On the Android emulator, host-loopback addresses (127.0.0.1, localhost)
 * resolve to the emulator itself, not the host. The Android emulator exposes
 * the host machine's loopback at the special address 10.0.2.2. Apps configured
 * with a localhost dev URL would otherwise fail to reach a host-side Jazz dev
 * server. Pass-through for non-Android platforms (iOS simulator's localhost
 * already resolves to the host).
 */
function resolveServerUrlForPlatform(serverUrl: string | undefined): string | undefined {
  if (!serverUrl || Platform.OS !== "android") return serverUrl;
  return serverUrl.replace(/(^https?:\/\/)(127\.0\.0\.1|localhost)(?=[:/]|$)/i, "$110.0.2.2");
}

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
      const tier = this.nativeConfig.tier ?? "local";
      const serverUrl = resolveServerUrlForPlatform(this.nativeConfig.serverUrl);
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
          serverUrl,
          env: this.nativeConfig.env,
          userBranch: this.nativeConfig.userBranch,
          jwtToken: this.nativeConfig.jwtToken,
          adminSecret: this.nativeConfig.adminSecret,
          tier,
          defaultDurabilityTier: "local",
        },
        {
          onAuthFailure: (reason) => {
            this.markUnauthenticated(reason);
          },
        },
      );

      if (serverUrl) {
        client.connectTransport(serverUrl, {
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

  // The base implementation needs a WASM module to mint the proof. RN has no
  // WASM module, so route through jazz-rn's native binding instead.
  override async getLocalFirstIdentityProof(options?: {
    ttlSeconds?: number;
    audience?: string;
  }): Promise<string | null> {
    const secret = this.nativeConfig.secret;
    if (!secret) return null;
    const ttl = BigInt(options?.ttlSeconds ?? 60);
    const audience = options?.audience ?? this.nativeConfig.appId;
    return jazzRn.jazz_rn.mintLocalFirstToken(secret, audience, ttl);
  }
}

export async function createDb(config: DbConfig): Promise<Db> {
  if (config.secret) {
    const jwtToken = jazzRn.jazz_rn.mintLocalFirstToken(config.secret, config.appId, BigInt(3600));
    return new Db({ ...config, jwtToken });
  }
  return new Db(config);
}
