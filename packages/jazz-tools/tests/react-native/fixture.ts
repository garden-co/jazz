import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb, type Db, type DbConfig } from "../../src/react-native/create-db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { Session } from "../../src/runtime/context.js";
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
  const nativeHost = createPlatformHost();
  const databases: Db[] = [];
  let closePromise: Promise<void> | undefined;
  const close = () =>
    (closePromise ??= (async () => {
      const errors: unknown[] = [];
      for (const cleanup of [
        ...databases.map((db) => () => db.shutdown()),
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
    const schema = serializeSchemaSource(app.wasmSchema);
    const capability = options.upstream
      ? nativeHost.attachCanonicalSchema(
          nativeHost.beginPrivateSession(
            JSON.stringify({
              server_url: options.upstream.serverUrl,
              app_id: appId,
              jwt: options.upstream.jwt,
              storage_root: directory,
            }),
          ),
          schema,
        )
      : nativeHost.admit(
          JSON.stringify({
            scope: {
              app_namespace: appId,
              storage_namespace: "default",
              auth_scope: JSON.stringify([session.issuer, session.user_id]),
            },
            sqlite_path: join(directory, "relay.sqlite"),
            schema_json: schema,
            identity: {
              node: randomUUID(),
              author: JSON.stringify([session.issuer, session.user_id]),
            },
            claims: session.claims,
          }),
        );
    const config: DbConfig = { appId, nativeRelay: { capability }, cookieSession: session };
    return {
      nativeHost,
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
