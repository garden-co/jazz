import type { LocalNode, SyncMessage } from "cojson";
import { onTestFinished } from "vitest";
import { StorageManagerAsync } from "../managerAsync";

export function trackMessages(node: LocalNode) {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalHandleSyncMessage =
    StorageManagerAsync.prototype.handleSyncMessage;
  const originalNodeSyncMessage = node.syncManager.handleSyncMessage;

  StorageManagerAsync.prototype.handleSyncMessage = async function (msg) {
    messages.push({
      from: "client",
      msg,
    });
    return originalHandleSyncMessage.call(this, msg);
  };

  node.syncManager.handleSyncMessage = async function (msg, peer) {
    messages.push({
      from: "storage",
      msg,
    });
    return originalNodeSyncMessage.call(this, msg, peer);
  };

  const restore = () => {
    StorageManagerAsync.prototype.handleSyncMessage = originalHandleSyncMessage;
    node.syncManager.handleSyncMessage = originalNodeSyncMessage;
  };

  onTestFinished(() => {
    restore();
  });

  return {
    messages,
    restore,
  };
}
export function waitFor(
  callback: () => boolean | undefined | Promise<boolean | undefined>,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();

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
