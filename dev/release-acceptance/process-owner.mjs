import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

/** Own a POSIX process group per command, including npm's non-detached descendants. */
export function createProcessOwner({ graceMs = 1500, killWaitMs = 1500 } = {}) {
  if (process.platform === "win32")
    throw new Error("Release acceptance requires POSIX process groups");
  const children = new Map();
  let closing = false,
    cleanupPromise,
    exiting = false;
  function signal(record, name) {
    if (!record.child.pid) return false;
    try {
      process.kill(-record.child.pid, name);
      return true;
    } catch (error) {
      if (error.code === "ESRCH") return false;
      throw error;
    }
  }
  async function stop(child) {
    const record = children.get(child);
    if (!record) return;
    if (!record.stopping)
      record.stopping = (async () => {
        if (signal(record, "SIGTERM")) {
          const deadline = Date.now() + graceMs;
          while (Date.now() < deadline && signal(record, 0)) await delay(25);
          if (signal(record, 0)) signal(record, "SIGKILL");
        }
        // Reap our direct child after escalation, without waiting forever on a
        // grandchild zombie that the host's init process has yet to reap.
        let timer;
        try {
          await Promise.race([
            record.exited,
            new Promise((resolve) => {
              timer = setTimeout(resolve, killWaitMs);
            }),
          ]);
        } finally {
          clearTimeout(timer);
          children.delete(child);
        }
      })();
    return record.stopping;
  }
  function cleanup() {
    closing = true;
    cleanupPromise ??= Promise.all([...children.keys()].map(stop));
    return cleanupPromise;
  }
  async function terminate(code) {
    if (exiting) return;
    exiting = true;
    process.exitCode = code;
    try {
      await cleanup();
    } finally {
      process.exit(code);
    }
  }
  const onInterrupt = () => {
    void terminate(130);
  };
  const onTerminate = () => {
    void terminate(143);
  };
  process.on("SIGINT", onInterrupt);
  process.on("SIGTERM", onTerminate);
  return {
    spawn(command, args, options) {
      if (closing) throw new Error("Acceptance process owner is shutting down");
      const child = spawn(command, args, { ...options, detached: true });
      const exited = new Promise((resolve) => {
        child.once("exit", resolve);
        child.once("error", resolve);
      });
      children.set(child, { child, exited });
      return child;
    },
    stop,
    cleanup,
    terminate,
    dispose() {
      process.off("SIGINT", onInterrupt);
      process.off("SIGTERM", onTerminate);
    },
  };
}
