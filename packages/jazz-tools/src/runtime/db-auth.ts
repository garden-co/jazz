import type { DbConfig } from "./db.js";
import type { DbRuntimeModule, RuntimeTokenOptions } from "./db-runtime-module.js";

const LOCAL_FIRST_TOKEN_TTL_SECONDS = 3600;
const LOCAL_FIRST_REFRESH_RATIO = 0.8;
const IDENTITY_PROOF_TTL_SECONDS = 60;

type TokenMintingRuntime<RuntimeConfig extends DbConfig> = Pick<
  DbRuntimeModule<RuntimeConfig>,
  "mintLocalFirstToken" | "mintAnonymousToken"
>;

type LocalFirstTokenRuntime<RuntimeConfig extends DbConfig> = Pick<
  DbRuntimeModule<RuntimeConfig>,
  "mintLocalFirstToken"
>;

export interface ResolvedDbAuthConfig<RuntimeConfig extends DbConfig> {
  config: RuntimeConfig & DbConfig;
  localFirstSecret: string | null;
}

export interface LocalFirstAuthManagerOptions<RuntimeConfig extends DbConfig = DbConfig> {
  appId: string;
  secret: string;
  runtimeModule: LocalFirstTokenRuntime<RuntimeConfig>;
  applyToken: (jwtToken: string) => void;
  isShuttingDown: () => boolean;
  ttlSeconds?: number;
}

export interface LocalFirstIdentityProofOptions {
  ttlSeconds?: number;
  audience?: string;
}

export function validateDbAuthConfig(config: DbConfig): void {
  if (config.secret && (config.jwtToken || config.cookieSession)) {
    throw new Error("DbConfig error: secret, jwtToken, and cookieSession are mutually exclusive");
  }
  if (config.jwtToken && config.cookieSession) {
    throw new Error("DbConfig error: jwtToken and cookieSession are mutually exclusive");
  }
}

export function resolveDbAuthConfig<RuntimeConfig extends DbConfig>(
  config: RuntimeConfig,
  runtimeModule: TokenMintingRuntime<RuntimeConfig>,
): ResolvedDbAuthConfig<RuntimeConfig> {
  validateDbAuthConfig(config);

  if (config.secret) {
    const jwtToken = runtimeModule.mintLocalFirstToken(
      createRuntimeTokenOptions(config.secret, config.appId, LOCAL_FIRST_TOKEN_TTL_SECONDS),
    );
    return {
      config: { ...config, jwtToken },
      localFirstSecret: config.secret,
    };
  }

  if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const jwtToken = runtimeModule.mintAnonymousToken(
      createRuntimeTokenOptions(ephemeralSeed, config.appId, LOCAL_FIRST_TOKEN_TTL_SECONDS),
    );

    return {
      config: { ...config, jwtToken },
      localFirstSecret: null,
    };
  }

  return {
    config: { ...config },
    localFirstSecret: null,
  };
}

export class LocalFirstAuthManager<RuntimeConfig extends DbConfig = DbConfig> {
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly ttlSeconds: number;

  constructor(private readonly options: LocalFirstAuthManagerOptions<RuntimeConfig>) {
    this.ttlSeconds = options.ttlSeconds ?? LOCAL_FIRST_TOKEN_TTL_SECONDS;
  }

  start(): void {
    this.scheduleRefresh();
  }

  stop(): void {
    if (!this.refreshTimer) {
      return;
    }
    clearTimeout(this.refreshTimer);
    this.refreshTimer = null;
  }

  getIdentityProof(options?: LocalFirstIdentityProofOptions): string {
    return this.mintToken(
      options?.audience ?? this.options.appId,
      options?.ttlSeconds ?? IDENTITY_PROOF_TTL_SECONDS,
    );
  }

  private scheduleRefresh(): void {
    this.stop();
    const refreshMs = this.ttlSeconds * LOCAL_FIRST_REFRESH_RATIO * 1000;
    this.refreshTimer = setTimeout(() => {
      this.refresh();
    }, refreshMs);
  }

  private refresh(): void {
    if (this.options.isShuttingDown()) {
      return;
    }

    try {
      const newToken = this.mintToken(this.options.appId, this.ttlSeconds);
      this.options.applyToken(newToken);
      this.scheduleRefresh();
    } catch (e) {
      console.error("Failed to refresh local-first token:", e);
    }
  }

  private mintToken(audience: string, ttlSeconds: number): string {
    return this.options.runtimeModule.mintLocalFirstToken(
      createRuntimeTokenOptions(this.options.secret, audience, ttlSeconds),
    );
  }
}

function createRuntimeTokenOptions(
  secret: string,
  audience: string,
  ttlSeconds: number,
): RuntimeTokenOptions {
  return {
    secret,
    audience,
    ttlSeconds,
    nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
  };
}

function generateEphemeralSeedBase64Url(): string {
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
