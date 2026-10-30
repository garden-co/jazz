/**
 * Tracks which dev-plugin instances currently hold a process-wide managed
 * runtime, and disposes the runtime only when the last one lets go.
 *
 * Vite's `restartServer` (triggered by `.env` or `vite.config.ts` changes)
 * creates the replacement server first — its fresh plugin instances run
 * `config`/`configureServer` and adopt the already-running runtime — and only
 * then closes the old server. Disposing on the old server's close would stop
 * the runtime the new server has just adopted, so each instance acquires the
 * runtime before initialising it and releases it on close.
 */
export class RuntimeOwners<Server> {
  private holders = new Map<object, Server | null>();
  private disposal: Promise<void> | null = null;

  constructor(private readonly disposeRuntime: () => Promise<void>) {}

  /**
   * Take ownership before (re)initialising the runtime. Waits for an in-flight
   * final disposal first so the caller starts a fresh runtime rather than
   * adopting one that is shutting down. A failed disposal was already reported
   * to the closer, so it does not block the next start.
   */
  async acquire(holder: object): Promise<void> {
    while (this.disposal) await this.disposal.catch(() => undefined);
    if (!this.holders.has(holder)) this.holders.set(holder, null);
  }

  /** Route runtime callbacks (schema reloads, push errors) to this holder's server. */
  attachServer(holder: object, server: Server): void {
    if (this.holders.has(holder)) this.holders.set(holder, server);
  }

  /** Servers of every current holder, oldest first. */
  servers(): Server[] {
    return [...this.holders.values()].filter((server): server is Server => server !== null);
  }

  /**
   * Drop this holder's ownership. The last holder out disposes the runtime;
   * concurrent releases share that one disposal. Releasing without holding is
   * a no-op (for example `closeBundle` after `vite build`).
   */
  release(holder: object): Promise<void> {
    if (!this.holders.delete(holder)) return this.disposal ?? Promise.resolve();
    if (this.holders.size > 0) return Promise.resolve();

    const pending = (async () => {
      try {
        await this.disposeRuntime();
      } finally {
        this.disposal = null;
      }
    })();
    this.disposal = pending;
    return pending;
  }

  /** Forget every holder without disposing; the caller resets the runtime itself. */
  resetForTests(): void {
    this.holders.clear();
    this.disposal = null;
  }
}
