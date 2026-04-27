import { createDb, type DbConfig } from "jazz-tools";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { readConfig } from "./config.js";
import {
  createIssueRepository,
  type IssueItem,
  type ItemStatus,
  type ListedItem,
  type ListFilters,
} from "./repository.js";

export interface IssueDataRepository {
  upsertItem(item: IssueItem): Promise<ListedItem>;
  listItems(filters?: ListFilters): Promise<ListedItem[]>;
  getItem(slug: string): Promise<ListedItem | null>;
  assignMe(slug: string): Promise<ListedItem>;
  setStatus(slug: string, status: ItemStatus): Promise<ListedItem>;
}

export async function openRepository(
  cwd: string,
  env: NodeJS.ProcessEnv,
): Promise<IssueDataRepository> {
  const config = await readConfig(cwd, env);
  const dbConfig = {
    secret: config.localFirstSecret,
    app,
    permissions,
    serverUrl: config.serverUrl,
    appId: config.appId,
  } satisfies DbConfig & {
    app: typeof app;
    permissions: typeof permissions;
  };
  const db = await createDb(dbConfig);

  return createIssueRepository(db, app);
}

function requiredEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

export async function openBackendRepository(env: NodeJS.ProcessEnv = process.env) {
  const context = createJazzContext({
    app,
    permissions,
    driver: { type: "memory" },
    appId: requiredEnv(env, "SKILL_ISSUES_APP_ID"),
    serverUrl: requiredEnv(env, "SKILL_ISSUES_SERVER_URL"),
    backendSecret: requiredEnv(env, "SKILL_ISSUES_BACKEND_SECRET"),
    env: "dev",
    userBranch: "main",
  });

  return createIssueRepository(context.db(), app);
}
