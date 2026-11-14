import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb, type Db, type DbConfig } from "../../src/react-native/create-db.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../../src/schema-source.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../../src/schema-permissions.js";
import type { Session } from "../../src/runtime/context.js";
import type { TestJazz } from "../../src/testing/index.js";
import { createAccountManagerWithRuntime } from "../../src/accounts/enrollment.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import { shippedMigrations } from "../../src/runtime/client-migrations.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

export interface NativeRelayFixtureOptions {
  appId?: string;
  session?: Session;
  /**
   * Sync with the app's backend (`startTestJazz`) as `userId`, signed in
   * with the app's bearer for them, through the production account ABI.
   * `serverUrl` (default the backend's) can route through a proxy.
   */
  upstream?: {
    backend: TestJazz;
    userId: string;
    claims?: Record<string, unknown>;
    serverUrl?: string;
  };
}
// Factory installation is a process-global platform action. Serializing only
// Db creation prevents concurrent fixtures selecting another fixture's host;
// each loaded RN RuntimeSource then retains its own factory independently.
let factoryCreation: Promise<unknown> = Promise.resolve();

export async function createNativeRelayFixture(
  app: SchemaSourceInput,
  permissions: CompiledPermissionsMap,
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
    if (options.upstream && options.session)
      throw new Error("An upstream RN fixture's session is the one its backend signs in");
    const upstream = options.upstream && {
      serverUrl: options.upstream.serverUrl ?? options.upstream.backend.url,
      bearer: options.upstream.backend.bearer(options.upstream.userId, options.upstream.claims),
    };
    let session: Session = options.session ?? {
      issuer: "https://auth.example",
      user_id: "rn-api-test",
      claims: {},
      authMode: "external" as const,
    };
    const serverUrl = upstream?.serverUrl ?? "https://edge.example";
    const registry = accountRegistryUrl(serverUrl, appId);
    // The app's own token for its backend: the real backend's (upstream), or
    // one the offline stand-in below reads the subject from.
    let bearer = upstream?.bearer ?? `fixture-bearer:${session.user_id}`;
    const assignments = new Map<string, string>();
    const manager = createAccountManagerWithRuntime({
      registry,
      localFirst: {
        create() {
          throw new Error("External fixture has no local-first key");
        },
      },
      bearer: () => bearer,
      // Only the backend's sign-in answer is substituted offline. All database
      // operations still traverse the production native account boundary.
      ...(!options.upstream
        ? {
            fetch: (async (_url, init) => {
              const token = new Headers(init?.headers).get("Authorization")!.slice(7);
              const subject = token.slice("fixture-bearer:".length);
              let id = assignments.get(subject);
              if (!id) {
                id =
                  subject === session.user_id ? (session.account_id ?? randomUUID()) : randomUUID();
                assignments.set(subject, id);
              }
              return new Response(
                JSON.stringify({
                  account: id,
                  identity: { issuer: session.issuer, subject },
                  claims: session.claims,
                }),
                { status: 200 },
              );
            }) as typeof fetch,
          }
        : {}),
    });
    const account = await manager.loginWithBackend();
    if (options.upstream)
      session = {
        issuer: account.identity.issuer,
        user_id: account.identity.subject,
        claims: options.upstream.claims ?? {},
        authMode: "external",
      };
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
          bearer,
          claims: session.claims,
          server_url: upstream?.serverUrl ?? null,
        }),
      ),
      serializeSchemaSource(
        mergePermissionsIntoWasmSchema(resolveSchemaSource(app), permissions),
        shippedMigrations(resolveSchemaSource(app)),
      ),
    );
    cleanupCapability = capability;
    const config: DbConfig = { appId, account, ...(options.upstream ? { serverUrl } : {}) };
    return {
      nativeHost,
      manager,
      async loginOriginal(): Promise<DbConfig> {
        bearer = upstream?.bearer ?? `fixture-bearer:${session.user_id}`;
        return { ...config, account: await manager.loginWithBackend() };
      },
      /** Sign another user in through the (offline) backend. */
      async registerIdentity(subject: string): Promise<DbConfig> {
        if (options.upstream) throw new Error("Real upstream fixtures need a real bearer");
        bearer = `fixture-bearer:${subject}`;
        return { ...config, account: await manager.loginWithBackend() };
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
  app: SchemaSourceInput,
  permissions: CompiledPermissionsMap,
  run: (fixture: NativeRelayFixture) => Promise<T>,
  options?: NativeRelayFixtureOptions,
): Promise<T> {
  const fixture = await createNativeRelayFixture(app, permissions, options);
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
