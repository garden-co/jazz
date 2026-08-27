export const PRESENCE_HEARTBEAT_INTERVAL_MS = 5_000;

/**
 * Starts a heartbeat after the enclosing React effect has committed. Deferring
 * the first write lets Strict Mode clean up its probe effect before it can
 * publish, while normal mounts still publish without waiting for five seconds.
 */
export function schedulePresenceHeartbeat(publish: () => void) {
  const initial = globalThis.setTimeout(publish, 0);
  const interval = globalThis.setInterval(publish, PRESENCE_HEARTBEAT_INTERVAL_MS);

  return () => {
    globalThis.clearTimeout(initial);
    globalThis.clearInterval(interval);
  };
}
