import type { LocalNode, RawCoID, SyncMessage } from "cojson";
import { StorageApiSync } from "cojson";
import { onTestFinished } from "vitest";

export function trackMessages() {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalLoad = StorageApiSync.prototype.load;
  const originalStore = StorageApiSync.prototype.store;

  StorageApiSync.prototype.load = async function (id, callback, done) {
    messages.push({
      from: "client",
      msg: {
        action: "load",
        id: id as RawCoID,
        header: false,
        sessions: {},
      },
    });
    return originalLoad.call(
      this,
      id,
      (msg) => {
        messages.push({
          from: "storage",
          msg,
        });
        callback(msg);
      },
      done,
    );
  };

  StorageApiSync.prototype.store = function (data, correctionCallback) {
    for (const msg of data) {
      messages.push({
        from: "client",
        msg,
      });
    }
    return originalStore.call(this, data, (msg) => {
      messages.push({
        from: "storage",
        msg: {
          action: "known",
          isCorrection: true,
          ...msg,
        },
      });
      correctionCallback(msg);
    });
  };

  const restore = () => {
    StorageApiSync.prototype.load = originalLoad;
    StorageApiSync.prototype.store = originalStore;
    messages.length = 0;
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
