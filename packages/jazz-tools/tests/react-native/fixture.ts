import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb, type Db, type DbConfig } from "../../src/react-native/create-db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { Session } from "../../src/runtime/context.js";
import { createAccountManagerWithRuntime } from "../../src/accounts/enrollment.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import {
  internalSessionFromJwtPayload,
  parseJwtPayload,
} from "../../src/runtime/client-session.js";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

export interface NativeRelayFixtureOptions {
  appId?: string;
  session?: Session;
  /** Real authenticated upstream, using the production private-session ABI. */
  upstream?: { serverUrl: string; jwt: string };
}
// Factory installation is a process-global platform action. Serializing only
// Db creation prevents concurrent fixtures selecting another fixture's host;
// each loaded RN RuntimeSource then retains its own factory independently.
let factoryCreation: Promise<unknown> = Promise.resolve();

export async function createNativeRelayFixture(
  app: { wasmSchema: WasmSchema },
  options: NativeRelayFixtureOptions = {},
) {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-api-"));
  const nativeHost = createPlatformHost(directory);
  const databases: Db[] = [];
  let cleanupCapability: Uint8Array | undefined;
  let closePromise: Promise<void> | undefined;
  const close = () =>
    (closePromise ??= (async () => {
      const errors: unknown[] = [];
      for (const cleanup of [
        ...databases.map((db) => () => db.shutdown()),
        () => {
          if (cleanupCapability) nativeHost.revoke(cleanupCapability);
        },
        () => {
          nativeHost.close();
        },
        () => rm(directory, { recursive: true, force: true }),
      ]) {
        try {
          await cleanup();
        } catch (error) {
          errors.push(error);
        }
      }
      if (errors.length) throw new AggregateError(errors, "RN fixture cleanup failed");
    })());
  try {
    const appId = options.appId ?? `rn-api-${randomUUID()}`;
    if (options.upstream && !options.session)
      throw new Error("Upstream RN fixture requires its admitted public session");
    const session = options.session ?? {
      issuer: "https://auth.example",
      user_id: "rn-api-test",
      claims: {},
      authMode: "external" as const,
    };
    const serverUrl = options.upstream?.serverUrl ?? "https://edge.example";
    const registry = accountRegistryUrl(serverUrl, appId);
    const jwt =
      options.upstream?.jwt ??
      `e30.${Buffer.from(
        JSON.stringify({
          ...session.claims,
          iss: session.issuer,
          sub: session.user_id,
        }),
      ).toString("base64url")}.fixture-signature`;
    const assignments = new Map<string, string>();
    const manager = createAccountManagerWithRuntime({
      registry,
      localFirst: {
        create() {
          throw new Error("External fixture has no local-first key");
        },
      },
      // Only offline enrollment is substituted. All database operations still
      // traverse the production native account/foreground boundary.
      ...(!options.upstream
        ? {
            fetch: (async (_url, init) => {
              const token = new Headers(init?.headers).get("Authorization")!.slice(7);
              const identity = parseJwtPayload(token)!;
              const key = JSON.stringify([identity.iss, identity.sub]);
              let id = assignments.get(key);
              if (!id) {
                id =
                  identity.iss === session.issuer && identity.sub === session.user_id
                    ? (session.account_id ?? randomUUID())
                    : randomUUID();
                assignments.set(key, id);
              }
              return new Response(
                JSON.stringify({
                  account: id,
                  identity: { issuer: identity.iss, subject: identity.sub },
                }),
                { status: 200 },
              );
            }) as typeof fetch,
          }
        : {}),
    });
    const account = await manager.registerJWT(jwt);
    const claims = internalSessionFromJwtPayload(parseJwtPayload(jwt)!)!.claims;
    // Keep a separate native admission for below-public-API ownership probes.
    // Public createDb calls below prepare their own account-bound admission.
    const capability = nativeHost.attachAccountSchema(
      nativeHost.beginAccountSession(
        JSON.stringify({
          registry,
          app_id: appId,
          env: "dev",
          account_id: account.id,
          issuer: account.identity.issuer,
          subject: account.identity.subject,
          jwt,
          claims,
          server_url: options.upstream?.serverUrl ?? null,
        }),
      ),
      serializeSchemaSource(app.wasmSchema),
    );
    cleanupCapability = capability;
    const config: DbConfig = { appId, account, ...(options.upstream ? { serverUrl } : {}) };
    return {
      nativeHost,
      manager,
      async loginOriginal(): Promise<DbConfig> {
        return { ...config, account: await manager.loginJWT(jwt) };
      },
      async registerIdentity(subject: string): Promise<DbConfig> {
        if (options.upstream) throw new Error("Real upstream fixtures need real provider tokens");
        const token = `e30.${Buffer.from(JSON.stringify({ iss: session.issuer, sub: subject })).toString("base64url")}.fixture-signature`;
        return { ...config, account: await manager.registerJWT(token) };
      },
      capability,
      config,
      directory,
      close,
      async createDb(dbConfig: DbConfig = config): Promise<Db> {
        const creation = factoryCreation.then(async () => {
          if (closePromise) throw new Error("RN fixture is closed");
          installPlatformHost(nativeHost);
          const db = await createDb(dbConfig);
          databases.push(db);
          return db;
        });
        factoryCreation = creation.catch(() => undefined);
        return creation;
      },
    };
  } catch (error) {
    try {
      await close();
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "RN fixture setup and cleanup failed");
    }
    throw error;
  }
}
export type NativeRelayFixture = Awaited<ReturnType<typeof createNativeRelayFixture>>;

export async function withNativeRelayFixture<T>(
  app: { wasmSchema: WasmSchema },
  run: (fixture: NativeRelayFixture) => Promise<T>,
  options?: NativeRelayFixtureOptions,
): Promise<T> {
  const fixture = await createNativeRelayFixture(app, options);
  const errors: unknown[] = [];
  let result: T | undefined;
  try {
    result = await run(fixture);
  } catch (error) {
    errors.push(error);
  }
  try {
    await fixture.close();
  } catch (error) {
    errors.push(error);
  }
  if (errors.length === 1) throw errors[0];
  if (errors.length) throw new AggregateError(errors, "RN fixture execution and cleanup failed");
  return result as T;
}
