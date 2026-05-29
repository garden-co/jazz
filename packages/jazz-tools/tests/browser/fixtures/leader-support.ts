export async function probeInSharedWorker(): Promise<boolean> {
  if (typeof SharedWorker === "undefined") return false;
  const worker = new SharedWorker(
    new URL("./leader-capability-probe.shared-worker.js", import.meta.url),
    { type: "module", name: `cap-${Math.random().toString(36).slice(2)}` },
  );
  const answer = await new Promise<boolean>((resolve) => {
    const timeout = setTimeout(() => resolve(false), 10000);
    worker.port.onmessage = (event: MessageEvent) => {
      if (event.data?.t === "PROBE_RESULT") {
        clearTimeout(timeout);
        resolve(Boolean(event.data.supported));
      }
    };
    worker.port.start();
    worker.port.postMessage({ t: "PROBE" });
  });
  try {
    worker.port.close();
  } catch {
    /* ignore */
  }
  return answer;
}

/**
 * Probed once per importing test module via top-level await (vitest browser
 * mode supports top-level await in ESM test files). Used with
 * `describe.skipIf(!leaderSupported)`.
 */
export const leaderSupported = await probeInSharedWorker();
