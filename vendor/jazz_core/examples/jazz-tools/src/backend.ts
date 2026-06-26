import { createDb, type Db, type DbOptions, type SchemaDefinition } from "./jazz-tools.js";

type AppWithSchema = {
  readonly _schema: SchemaDefinition;
};

export type JazzBackendDriver =
  | "memory"
  | "local"
  | "persistent"
  | {
      readonly kind?: "memory" | "local" | "persistent" | string;
      readonly type?: "memory" | "local" | "persistent" | string;
      readonly name?: string;
    };

export type JazzContextOptions = Omit<DbOptions, "schema"> & {
  readonly app?: AppWithSchema;
  readonly schema?: SchemaDefinition;
  readonly driver?: JazzBackendDriver;
  readonly serverUrl?: string;
  readonly backendSecret?: string;
  readonly adminSecret?: string;
  readonly allowLocalFirstAuth?: boolean;
  readonly env?: string;
  readonly userBranch?: string;
};

export type JazzBackend = {
  readonly db: Db;
  shutdown(): Promise<void>;
};

export type JazzContext = {
  db(): Db;
  asBackend(): JazzBackend;
  shutdown(): Promise<void>;
};
type CloseableDb = Db & {
  close(): Promise<void>;
};

export async function createJazzContext(options: JazzContextOptions): Promise<JazzContext> {
  assertSupportedDriver(options.driver);
  const schema = resolveSchema(options);
  const db = await createDb({ ...options, schema });
  const closeableDb = db as CloseableDb;
  const backend: JazzBackend = {
    db,
    async shutdown() {
      await closeableDb.close();
    },
  };

  return {
    db() {
      return db;
    },
    asBackend() {
      return backend;
    },
    shutdown() {
      return backend.shutdown();
    },
  };
}

function resolveSchema(options: JazzContextOptions): SchemaDefinition {
  if (options.schema) return options.schema;
  if (options.app) return options.app._schema;
  throw new Error("createJazzContext requires either schema or app with _schema.");
}

function assertSupportedDriver(driver: JazzBackendDriver | undefined): void {
  const driverKind = driverKindOf(driver);
  if (driverKind == null || driverKind === "memory" || driverKind === "local") return;
  throw new Error(
    `createJazzContext driver "${driverKind}" is not supported by this current jazz-tools/WasmDb slice. ` +
      'Use driver: "memory" or "local"; persistent backend storage is not exposed honestly yet.',
  );
}

function driverKindOf(driver: JazzBackendDriver | undefined): string | undefined {
  if (driver == null) return undefined;
  if (typeof driver === "string") return driver;
  return driver.kind ?? driver.type ?? driver.name;
}
