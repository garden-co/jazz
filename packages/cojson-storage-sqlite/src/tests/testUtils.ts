import type { LocalNode, SyncMessage } from "cojson";
import { SyncManager } from "cojson-storage";
import { onTestFinished } from "vitest";

export function trackMessages(node: LocalNode) {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalHandleSyncMessage = SyncManager.prototype.handleSyncMessage;
  const originalNodeSyncMessage = node.syncManager.handleSyncMessage;

  SyncManager.prototype.handleSyncMessage = async function (msg) {
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
    SyncManager.prototype.handleSyncMessage = originalHandleSyncMessage;
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
