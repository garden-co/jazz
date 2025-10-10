import { type AdapterDebugLogs, createAdapter } from "better-auth/adapters";
import type { Account } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import {
  JazzRepository,
  UserRepository,
  SessionRepository,
  VerificationRepository,
  AccountRepository,
} from "./repository";
import { createJazzSchema, tableItem2Record } from "./schema.js";

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
    adapter: ({ schema }) => {
      const JazzSchema = createJazzSchema(schema);

      let worker: Account | undefined = undefined;

      async function getWorker(): Promise<Account> {
        if (worker) {
          return worker;
        }

        const result = await startWorker({
          AccountSchema: JazzSchema.WorkerAccount,
          syncServer: config.syncServer,
          accountID: config.accountID,
          accountSecret: config.accountSecret,
          skipInboxLoad: true,
          asActiveAccount: false,
        });

        worker = result.worker;

        return result.worker;
      }

      async function initRepository(
        model: string,
        ensureSync: boolean = false,
      ): Promise<JazzRepository> {
        let Repository: typeof JazzRepository | undefined = undefined;
        switch (model) {
          case "user":
            Repository = UserRepository;
            break;
          case "session":
            Repository = SessionRepository;
            break;
          case "verification":
            Repository = VerificationRepository;
            break;
          case "account":
            Repository = AccountRepository;
            break;
        }

        if (!Repository) {
          Repository = JazzRepository;
        }

        const worker = await getWorker();
        const database = await JazzSchema.loadDatabase(worker);

        const repository = new Repository(
          JazzSchema.DatabaseRoot,
          database,
          worker,
          schema,
          ensureSync,
        );

        return repository;
      }

      return {
        create: async ({ data, model, select }): Promise<any> => {
          // console.log("create", { data, model, select });
          const repository = await initRepository(model, true);

          const created = await repository.create(model, data);

          await repository.ensureSync();

          return tableItem2Record(created);
        },
        update: async ({ model, where, update }): Promise<any> => {
          // console.log("update", { model, where, update });
          const repository = await initRepository(model, true);

          const updated = await repository.update(
            model,
            where,
            update as Record<string, any>,
          );

          if (updated.length === 0) {
            return null;
          }

          await repository.ensureSync();

          return tableItem2Record(updated[0]!);
        },
        updateMany: async ({ model, where, update }) => {
          // console.log("updateMany", { model, where, update });
          const repository = await initRepository(model, true);

          const updated = await repository.update(model, where, update);

          await repository.ensureSync();

          return updated.length;
        },
        delete: async ({ model, where }) => {
          // console.log("delete", { model, where });
          const repository = await initRepository(model, true);

          await repository.deleteValue(model, where);

          await repository.ensureSync();
        },
        findOne: async ({ model, where }): Promise<any> => {
          // console.log("findOne", { model, where });
          const repository = await initRepository(model);

          const item = await repository.findOne(model, where);

          return tableItem2Record(item);
        },
        findMany: async ({
          model,
          where,
          limit,
          sortBy,
          offset,
        }): Promise<any[]> => {
          const repository = await initRepository(model);

          const items = await repository.findMany(
            model,
            where,
            limit,
            sortBy,
            offset,
          );

          return items.map(tableItem2Record);
        },
        deleteMany: async ({ model, where }) => {
          const repository = await initRepository(model, true);

          const deleted = await repository.deleteValue(model, where);

          await repository.ensureSync();

          return deleted;
        },
        count: async ({ model, where }) => {
          // console.log("count", { model, where });
          const repository = await initRepository(model);

          return repository.count(model, where);
        },
      };
    },
  });
