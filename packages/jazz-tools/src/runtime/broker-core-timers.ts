/**
 * Timer registry shared by the broker-core I/O shells (the SharedWorker broker
 * and the tab client). The Rust cores address timers by structured key and
 * re-arm by setting the same key again, so `set` always clears any previous
 * timeout registered under that key.
 */
export interface CoreTimerKey {
  kind: string;
  [key: string]: unknown;
}

export interface CoreTimerRegistry<K extends CoreTimerKey> {
  set(timer: K, delayMs: number): void;
  clear(timer: K): void;
}

export function createCoreTimerRegistry<K extends CoreTimerKey>(
  onFire: (timer: K) => void,
): CoreTimerRegistry<K> {
  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  return {
    set(timer, delayMs) {
      const key = JSON.stringify(timer);
      clearTimeout(timers.get(key));
      timers.set(
        key,
        setTimeout(() => {
          timers.delete(key);
          onFire(timer);
        }, delayMs),
      );
    },
    clear(timer) {
      const key = JSON.stringify(timer);
      const handle = timers.get(key);
      if (handle === undefined) return;
      clearTimeout(handle);
      timers.delete(key);
    },
  };
}
