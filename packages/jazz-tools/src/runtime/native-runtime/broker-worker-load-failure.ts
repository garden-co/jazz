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

/** Vite's dependency optimizer serves pre-bundled packages from `<cacheDir>/deps/`. */
const VITE_PREBUNDLED_MODULE = /\/\.vite\/deps(?:_[^/]*)?\//;

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
 * for a path that doesn't exist. When the bundler chose the URL, Jazz can't
 * see it; the one cause it can name for sure is Vite 6/7 pre-bundling
 * jazz-tools, which resolves the worker's `new URL(..., import.meta.url)`
 * against the pre-bundled chunk in `.vite/deps` instead of the package.
 */
export async function describeBrokerWorkerLoadFailure(
  context: BrokerWorkerLoadFailureContext,
): Promise<BrowserWorkerScriptLoadError> {
  const prebundledByVite = VITE_PREBUNDLED_MODULE.test(safePathname(context.moduleUrl));
  const answer = context.workerUrl
    ? await describeServerAnswer(context.workerUrl, context.fetch ?? globalThis.fetch)
    : null;

  const what = context.workerUrl
    ? `Jazz's browser worker script ${context.workerUrl} did not load${answer ? `: ${answer}` : ""}.`
    : "Jazz's browser worker script did not load.";
  const why = prebundledByVite
    ? ' Vite pre-bundled jazz-tools, and Vite 6 and 7 point the worker URL of a pre-bundled package at a path that does not exist, which the dev server answers with index.html. Add "jazz-tools" to optimizeDeps.exclude in your Vite config, next to "jazz-wasm", or upgrade to Vite 8.'
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

function safePathname(url: string): string {
  try {
    return new URL(url).pathname;
  } catch {
    return url;
  }
}
