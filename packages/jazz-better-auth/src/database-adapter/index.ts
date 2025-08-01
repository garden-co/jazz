import { type AdapterDebugLogs, createAdapter } from "better-auth/adapters";
import { co } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import * as JazzRepository from "./jazz-repository.js";
import { createJazzSchema } from "./schema.js";

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

      let worker: co.loaded<typeof JazzSchema.WorkerAccount> | undefined =
        undefined;

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
        // createSchema: async ({ file, tables }) => {
        // },
        create: async ({ data, model, select }) => {
          // console.log({method: 'create', data, model, select });
          const schema = JazzSchema.dbSchema[model]!;

          const worker = await getWorker();
          return JazzRepository.create(worker, schema, model, data);
        },
        update: async ({ model, where, update }) => {
          // console.log({method: 'update', model, where, update });

          const worker = await getWorker();
          const updated = await JazzRepository.update(
            worker,
            model,
            where,
            update,
          );

          if (updated.length === 0) {
            return null;
          }

          return updated[0]!;
        },
        updateMany: async ({ model, where, update }) => {
          // console.log({method: 'updateMany', model, where, update });
          const worker = await getWorker();
          const updated = await JazzRepository.update(
            worker,
            model,
            where,
            update,
          );

          // console.log({updated});
          return updated.length;
        },
        delete: async ({ model, where }) => {
          // console.log({method: 'delete', model, where });

          const worker = await getWorker();
          await JazzRepository.deleteValue(worker, model, where);
          return;
        },
        findOne: async ({ model, where }) => {
          // console.log({method: 'findOne', model, where });
          const worker = await getWorker();
          return JazzRepository.findOne(worker, model, where);
        },
        findMany: async ({ model, where, limit, sortBy, offset }) => {
          // console.log({method: 'findMany', model, where, limit, sortBy, offset });

          const worker = await getWorker();
          return JazzRepository.findMany(
            worker,
            model,
            where,
            limit,
            sortBy,
            offset,
          );
        },
        deleteMany: async ({ model, where }) => {
          // console.log({method: 'deleteMany', model, where });

          const worker = await getWorker();
          return JazzRepository.deleteValue(worker, model, where);
        },
        count: async ({ model, where }) => {
          // console.log({method: 'count', model, where });
          const worker = await getWorker();
          return JazzRepository.count(worker, model, where);
        },
      };
    },
  });
