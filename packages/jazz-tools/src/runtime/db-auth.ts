import type { DbConfig } from "./db.js";
import type { BackendTokenOptions, DbBackendModule } from "./db-backend.js";

const LOCAL_FIRST_TOKEN_TTL_SECONDS = 3600;
const LOCAL_FIRST_REFRESH_RATIO = 0.8;
const IDENTITY_PROOF_TTL_SECONDS = 60;

type TokenMintingBackend<BackendConfig extends DbConfig> = Pick<
  DbBackendModule<BackendConfig>,
  "mintLocalFirstToken" | "mintAnonymousToken"
>;

type LocalFirstTokenBackend<BackendConfig extends DbConfig> = Pick<
  DbBackendModule<BackendConfig>,
  "mintLocalFirstToken"
>;

export interface ResolvedDbAuthConfig<BackendConfig extends DbConfig> {
  config: BackendConfig & DbConfig;
  localFirstSecret: string | null;
}

export interface LocalFirstAuthManagerOptions<BackendConfig extends DbConfig = DbConfig> {
  appId: string;
  secret: string;
  backend: LocalFirstTokenBackend<BackendConfig>;
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

export function resolveDbAuthConfig<BackendConfig extends DbConfig>(
  config: BackendConfig,
  backend: TokenMintingBackend<BackendConfig>,
): ResolvedDbAuthConfig<BackendConfig> {
  validateDbAuthConfig(config);

  if (config.secret) {
    const jwtToken = backend.mintLocalFirstToken(
      createBackendTokenOptions(config.secret, config.appId, LOCAL_FIRST_TOKEN_TTL_SECONDS),
    );
    return {
      config: { ...config, jwtToken },
      localFirstSecret: config.secret,
    };
  }

  if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const jwtToken = backend.mintAnonymousToken(
      createBackendTokenOptions(ephemeralSeed, config.appId, LOCAL_FIRST_TOKEN_TTL_SECONDS),
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

/**
 * Manages the in-memory token lifecycle for a Db created with a local-first secret.
 *
 * The manager does not persist, load, or rotate the secret itself. It only keeps
 * the resolved secret available for identity proofs and periodically mints a
 * fresh app auth token before the current one expires.
 */
export class LocalFirstAuthManager<BackendConfig extends DbConfig = DbConfig> {
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly ttlSeconds: number;

  constructor(private readonly options: LocalFirstAuthManagerOptions<BackendConfig>) {
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
    return this.options.backend.mintLocalFirstToken(
      createBackendTokenOptions(this.options.secret, audience, ttlSeconds),
    );
  }
}

function createBackendTokenOptions(
  secret: string,
  audience: string,
  ttlSeconds: number,
): BackendTokenOptions {
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
