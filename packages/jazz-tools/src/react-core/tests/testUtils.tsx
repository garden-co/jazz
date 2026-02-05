import {
  RenderHookOptions,
  RenderOptions,
  render,
  renderHook,
} from "@testing-library/react";
import { Account, AnonymousJazzAgent } from "jazz-tools";
import React from "react";
import { JazzTestProvider } from "../testing.js";
import { getSqliteStorageAsync, SQLiteDatabaseDriverAsync } from "cojson";
import Database, { type Database as DatabaseT } from "libsql";
import { onTestFinished } from "vitest";

type JazzExtendedOptions = {
  account?: Account | { guest: AnonymousJazzAgent };
  isAuthenticated?: boolean;
};

const customRender = (
  ui: React.ReactNode,
  options: RenderOptions & JazzExtendedOptions = {},
) => {
  const AllTheProviders = ({ children }: { children: React.ReactNode }) => {
    return (
      <JazzTestProvider
        account={options.account}
        isAuthenticated={options.isAuthenticated}
      >
        {options.wrapper ? (
          <options.wrapper>{children}</options.wrapper>
        ) : (
          children
        )}
      </JazzTestProvider>
    );
  };

  return render(ui, { ...options, wrapper: AllTheProviders });
};

const customRenderHook = <TProps, TResult>(
  callback: (props: TProps) => TResult,
  options: RenderHookOptions<TProps> & JazzExtendedOptions = {},
) => {
  const AllTheProviders = ({ children }: { children: React.ReactNode }) => {
    return (
      <JazzTestProvider
        account={options.account}
        isAuthenticated={options.isAuthenticated}
      >
        {options.wrapper ? (
          <options.wrapper>{children}</options.wrapper>
        ) : (
          children
        )}
      </JazzTestProvider>
    );
  };

  return renderHook(callback, { ...options, wrapper: AllTheProviders });
};

// re-export everything
export * from "@testing-library/react";

// override render method
export { customRender as render };
export { customRenderHook as renderHook };

class LibSQLSqliteAsyncDriver implements SQLiteDatabaseDriverAsync {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    this.db = new Database(filename, {});
  }

  async initialize() {
    await this.db.pragma("journal_mode = WAL");
  }

  async run(sql: string, params: unknown[]) {
    this.db.prepare(sql).run(params);
  }

  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    return this.db.prepare(sql).all(params) as T[];
  }

  async get<T>(sql: string, params: unknown[]): Promise<T | undefined> {
    return this.db.prepare(sql).get(params) as T | undefined;
  }

  async transaction(callback: (tx: LibSQLSqliteAsyncDriver) => unknown) {
    await this.run("BEGIN TRANSACTION", []);

    try {
      await callback(this);
      await this.run("COMMIT", []);
    } catch (error) {
      await this.run("ROLLBACK", []);
      throw error;
    }
  }

  async closeDb() {
    this.db.close();
  }
}

export async function createAsyncStorage() {
  const storage = await getSqliteStorageAsync(
    new LibSQLSqliteAsyncDriver(":memory:"),
  );

  onTestFinished(async () => {
    await storage.close();
  });

  return storage;
}
