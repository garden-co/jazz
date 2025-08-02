import type { RawCoID, SyncMessage } from "cojson";
import { StorageApiAsync } from "cojson";
import { onTestFinished } from "vitest";

export function trackMessages() {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalLoad = StorageApiAsync.prototype.load;
  const originalStore = StorageApiAsync.prototype.store;

  StorageApiAsync.prototype.load = async function (id, callback, done) {
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

  StorageApiAsync.prototype.store = async function (data, correctionCallback) {
    messages.push({
      from: "client",
      msg: data,
    });

    return originalStore.call(this, data, (msg) => {
      messages.push({
        from: "storage",
        msg: {
          action: "known",
          isCorrection: true,
          ...msg,
        },
      });
      const correctionMessages = correctionCallback(msg);

      if (correctionMessages) {
        for (const msg of correctionMessages) {
          messages.push({
            from: "client",
            msg,
          });
        }
      }

      return correctionMessages;
    });
  };

  const restore = () => {
    StorageApiAsync.prototype.load = originalLoad;
    StorageApiAsync.prototype.store = originalStore;
    messages.length = 0;
  };

  const clear = () => {
    messages.length = 0;
  };

  onTestFinished(() => {
    restore();
  });

  return {
    messages,
    restore,
    clear,
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
