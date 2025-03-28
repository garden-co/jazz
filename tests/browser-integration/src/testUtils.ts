import { commands } from "@vitest/browser/context";
import { internal_setDatabaseName } from "cojson-storage-indexeddb";
import {
  JazzBrowserContextManager,
  JazzContextManagerProps,
} from "jazz-browser";
import { Account, JazzContextManagerAuthProps } from "jazz-tools";
import { onTestFinished } from "vitest";

export function waitFor(callback: () => boolean | void) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = () => {
      try {
        return { ok: callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(() => {
      const { ok, error } = checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

export async function createAccountContext<Acc extends Account>(
  props: JazzContextManagerProps<Acc> & { databaseName?: string },
  authProps?: JazzContextManagerAuthProps,
) {
  internal_setDatabaseName(
    props.databaseName ?? Math.random().toString(36).substring(2, 15),
  );

  const contextManager = new JazzBrowserContextManager<Acc>();

  await contextManager.createContext(props, authProps);

  const value = contextManager.getCurrentValue();

  if (!value || !("me" in value)) {
    throw new Error("Account not found");
  }

  onTestFinished(async () => {
    contextManager.done();
  });

  return { context: value, account: value.me, contextManager };
}

export async function startSyncServer(port?: number, dbName?: string) {
  const { url, port: syncServerPort } = await commands.startSyncServer(
    port,
    dbName,
  );

  function close() {
    return commands.closeSyncServer(url);
  }

  onTestFinished(close);

  return {
    url,
    port: syncServerPort,
    disconnectAllClients: () => commands.disconnectAllClients(url),
    setOffline: (active: boolean) => commands.setOffline(url, active),
    close,
  };
}
