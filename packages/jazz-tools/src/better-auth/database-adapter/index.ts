import { type AdapterDebugLogs, createAdapter } from "better-auth/adapters";
import type { Account } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import * as JazzRepository from "./jazz-repository.js";
import { createJazzSchema } from "./schema.js";
import * as SessionRepository from "./session-repository.js";
import * as UserRepository from "./user-repository.js";
import * as VerificationRepository from "./verification-repository.js";

export interface JazzAdapterConfig {
  /**
   * Helps you debug issues with the adapter.
   */
  debugLogs?: AdapterDebugLogs;
  /**
   * The sync server to use.
   */
  syncServer: string;
  /**
   * The worker account ID to use.
   */
  accountID: string;
  /**
   * The worker account secret to use.
   */
  accountSecret: string;
}

type CustomRepository = Omit<
  typeof JazzRepository,
  "findByUnique" | "findById"
>;

const customRepository: Record<string, CustomRepository> = {
  session: SessionRepository,
  user: UserRepository,
  verification: VerificationRepository,
};

/**
 * Creates a Better Auth database adapter that integrates with Jazz framework.
 *
 * This adapter provides a seamless integration between Better Auth and Jazz,
 * allowing you to use Jazz as database for for Better Auth's authentication system.
 *
 * @param config - Configuration object for the Jazz Better Auth adapter
 * @param config.syncServer - The Jazz sync server URL to connect to (e.g., "wss://your-sync-server.com")
 * @param config.accountID - The worker account ID for the Jazz worker that will handle auth operations
 * @param config.accountSecret - The worker account secret for authenticating with the Jazz sync server
 * @param config.debugLogs - Optional debug logging configuration to help troubleshoot adapter issues
 *
 * @returns A Better Auth adapter instance configured to work with Jazz
 *
 * @example
 * ```typescript
 * import { JazzBetterAuthDatabaseAdapter } from "jazz-tools/better-auth/database-adapter";
 * import { createAuth } from "better-auth";
 *
 * const auth = createAuth({
 *   adapter: JazzBetterAuthDatabaseAdapter({
 *     syncServer: "wss://your-jazz-sync-server.com",
 *     accountID: "auth-worker-account-id",
 *     accountSecret: "your-worker-account-secret",
 *   }),
 *   // ... other auth configuration
 * });
 * ```
 */
export const JazzBetterAuthDatabaseAdapter = (
  config: JazzAdapterConfig,
): ReturnType<typeof createAdapter> =>
  createAdapter({
    config: {
      adapterId: "jazz-tools-adapter", // A unique identifier for the adapter.
      adapterName: "Jazz Tools Adapter", // The name of the adapter.
      debugLogs: config.debugLogs ?? false, // Whether to enable debug logs.
      supportsJSON: true, // Whether the database supports JSON. (Default: false)
      supportsDates: true, // Whether the database supports dates. (Default: true)
      supportsBooleans: true, // Whether the database supports booleans. (Default: true)
      supportsNumericIds: false, // Whether the database supports auto-incrementing numeric IDs. (Default: true)
      disableIdGeneration: true,
    },
    // @ts-expect-error TODO: fix generic type
    adapter: ({ schema }) => {
      const JazzSchema = createJazzSchema(schema);

      let worker: Account | undefined = undefined;

      async function getWorker() {
        if (worker) {
          return worker;
        }

        const result = await startWorker({
          AccountSchema: JazzSchema.WorkerAccount,
          syncServer: config.syncServer,
          accountID: config.accountID,
          accountSecret: config.accountSecret,
        });

        worker = result.worker;

        return worker;
      }

      return {
        create: async ({ data, model, select }) => {
          // console.log("create", { data, model, select });
          const schema =
            JazzSchema.DatabaseRoot.shape.tables.shape[model]?.element;

          if (!schema) {
            throw new Error(`Schema for model ${model} not found`);
          }

          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          return method.create(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            schema,
            model,
            data,
          );
          // console.log("update", { model, where, update });
        },
        update: async ({ model, where, update }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          const updated = await method.update(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
            update,
          );

          if (updated.length === 0) {
            return null;
          }

          return updated[0]!;
          // console.log("updateMany", { model, where, update });
        },
        updateMany: async ({ model, where, update }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          const updated = await method.update(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
            update,
          );

          return updated.length;
          // console.log("delete", { model, where });
        },
        delete: async ({ model, where }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          await method.deleteValue(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
          );
          // console.log("findOne", { model, where });
        },
        findOne: async ({ model, where }) => {
          const worker = await getWorker();

          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          return method.findOne(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
          );
        },
        findMany: async ({ model, where, limit, sortBy, offset }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          return method.findMany(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            // console.log("deleteMany", { model, where });
            where,
            limit,
            sortBy,
            offset,
          );
        },
        // console.log("count", { model, where });
        deleteMany: async ({ model, where }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          return method.deleteValue(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
          );
        },
        count: async ({ model, where }) => {
          const worker = await getWorker();
          const database = await JazzSchema.loadDatabase(worker);

          const method = customRepository[model] ?? JazzRepository;

          return method.count(
            {
              schema: JazzSchema.DatabaseRoot,
              db: database,
            },
            model,
            where,
          );
        },
      };
    },
  });
