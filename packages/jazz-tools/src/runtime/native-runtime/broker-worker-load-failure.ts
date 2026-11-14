import { vitePrebundledBrokerWorkerUrl } from "../browser-worker-config.js";

/**
 * The browser could not start Jazz's broker SharedWorker: its script failed
 * to load or evaluate, so no Jazz code ever ran in the worker realm.
 *
 * Every engine reports this as an `error` event on the `SharedWorker` object
 * within milliseconds. Without listening for it, the page only learns of it
 * from the foreground-lease admission timeout, ten seconds later and with no
 * cause.
 */
export class BrowserWorkerScriptLoadError extends Error {
  override readonly name = "BrowserWorkerScriptLoadError";
}

export interface BrokerWorkerLoadFailureContext {
  /** The worker script URL, when Jazz computed it rather than the bundler. */
  workerUrl?: string;
  /** `import.meta.url` of the Jazz module that constructed the worker. */
  moduleUrl: string;
  fetch?: typeof fetch;
}

/**
 * Explain a broker worker that never started.
 *
 * When Jazz knows the script URL (configured runtime sources), fetch it once
 * to say what the server answered: an HTML page is a single-page-app fallback
 * for a path that doesn't exist. Under the Vite dev server's pre-bundle, Jazz
 * resolves the worker through the dev server (see
 * `vitePrebundledBrokerWorkerUrl`), so the remaining cause there is a
 * jazz-tools the dev server can't resolve from the project. When a bundler
 * chose the URL, Jazz can't see it.
 */
export async function describeBrokerWorkerLoadFailure(
  context: BrokerWorkerLoadFailureContext,
): Promise<BrowserWorkerScriptLoadError> {
  const prebundledByVite = vitePrebundledBrokerWorkerUrl(context.moduleUrl) !== undefined;
  const answer = context.workerUrl
    ? await describeServerAnswer(context.workerUrl, context.fetch ?? globalThis.fetch)
    : null;

  const what = context.workerUrl
    ? `Jazz's browser worker script ${context.workerUrl} did not load${answer ? `: ${answer}` : ""}.`
    : "Jazz's browser worker script did not load.";
  const why = prebundledByVite
    ? ' Jazz runs from the Vite dev server\'s dependency pre-bundle, so it asks the dev server to resolve the worker from the jazz-tools package; that needs jazz-tools to be a dependency the dev server resolves from your project. Otherwise, add "jazz-tools" to optimizeDeps.exclude in your Vite config, next to "jazz-wasm".'
    : context.workerUrl
      ? " Serve jazz-tools' dist/worker/jazz-broker-worker.js at that URL as JavaScript, or point runtimeSources.brokerWorkerUrl at where it is served."
      : " Your bundler or server did not serve jazz-tools' dist/worker/jazz-broker-worker.js as JavaScript; a single-page-app fallback that answers unknown paths with index.html is a common cause. Point runtimeSources.brokerWorkerUrl at where the worker is served.";
  return new BrowserWorkerScriptLoadError(
    `${what}${why} The local-first client cannot start without it.`,
  );
}

async function describeServerAnswer(url: string, fetchImpl?: typeof fetch): Promise<string | null> {
  if (!fetchImpl) return null;
  try {
    const response = await fetchImpl(url, { cache: "no-store" });
    void response.body?.cancel().catch(() => undefined);
    const contentType = response.headers.get("content-type") ?? "";
    if (/\btext\/html\b/i.test(contentType)) {
      return `the server answered with an HTML page (HTTP ${response.status}) instead of JavaScript`;
    }
    if (!response.ok) return `the server answered HTTP ${response.status}`;
    return null;
  } catch {
    return null;
  }
}
