import { access } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect } from "vitest";
import { createJazzContext, Db, Session, type JazzContext } from "../backend/index.js";
import {
  pushSchemaCatalogue,
  startLocalJazzServer,
  type LocalJazzServerHandle,
} from "./local-jazz-server.js";

/**
 * A test app for permissions tests. Simplifies setting up a test app and provides methods
 * for seeding the database and validating policy checks.
 */
export class PolicyTestApp {
  constructor(
    private readonly app: any,
    private readonly jazzContext: JazzContext,
    private readonly server: LocalJazzServerHandle,
  ) {}

  /**
   * Seed the database with the given callback.
   * The callback is executed in an admin database context.
   */
  seed<T>(callback: (db: Db) => T): T {
    const db = this.jazzContext.asBackend(this.app);
    return callback(db);
  }

  /**
   * Get a database client for the given session.
   */
  as(session: Session): Db {
    return this.jazzContext.forSession(session, this.app);
  }

  /**
   * Assert that the callback does not throw a policy error.
   * TODO: rollback mutations performed as part of the callback (once we support transactions).
   */
  expectAllowed(callback: () => unknown): void {
    expect(callback).not.toThrow();
  }

  /**
   * Assert that the callback throws a policy error.
   * TODO: rollback mutations performed as part of the callback (once we support transactions).
   */
  expectDenied(callback: () => unknown): void {
    expect(callback).toThrow('WriteError("policy denied');
  }

  /**
   * Shutdown the test app. This will stop the local Jazz client and server.
   */
  async shutdown(): Promise<void> {
    await this.jazzContext.shutdown();
    await this.server.stop();
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function resolvePolicyTestSchemaPaths(schemaDir: string): Promise<{
  catalogueDir: string;
  appModulePath: string;
}> {
  const directAppModule = join(schemaDir, "app.js");
  if (await pathExists(directAppModule)) {
    return {
      catalogueDir: schemaDir,
      appModulePath: directAppModule,
    };
  }

  for (const extension of ["ts", "js"]) {
    const directRootSchema = join(schemaDir, `schema.${extension}`);
    if (await pathExists(directRootSchema)) {
      return {
        catalogueDir: schemaDir,
        appModulePath: directRootSchema,
      };
    }
  }

  if (basename(schemaDir) === "schema") {
    const appRoot = dirname(schemaDir);
    for (const extension of ["ts", "js"]) {
      const parentRootSchema = join(appRoot, `schema.${extension}`);
      if (await pathExists(parentRootSchema)) {
        return {
          catalogueDir: appRoot,
          appModulePath: parentRootSchema,
        };
      }
    }
  }

  throw new Error(
    `Could not find a schema app near ${schemaDir}. Expected app.js, schema.ts, or schema.js.`,
  );
}

/**
 * Create a new policy test app.
 * This will start a local Jazz server and push the schema catalogue to it.
 * Returns a PolicyTestApp instance that can be used to seed the database and validate policy checks.
 * @param schemaDir - The directory containing the Jazz schema and permissions
 */
export async function createPolicyTestApp(schemaDir: string): Promise<PolicyTestApp> {
  const backendSecret = `backend-secret`;
  const adminSecret = `admin-secret`;
  const resolvedPaths = await resolvePolicyTestSchemaPaths(schemaDir);
  const server = await startLocalJazzServer({
    backendSecret,
    adminSecret,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret,
    schemaDir: resolvedPaths.catalogueDir,
    env: "test",
    userBranch: "main",
  });

  const app = await import(pathToFileURL(resolvedPaths.appModulePath).href);
  if (!app) {
    throw new Error(`No schema app module found near ${schemaDir}`);
  }
  const jazzContext = createJazzContext({
    appId: server.appId,
    app,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
    tier: "worker",
  });

  return new PolicyTestApp(app, jazzContext, server);
}
