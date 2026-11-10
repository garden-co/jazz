/**
 * The agent brief lives only on the docs site, so an old create-jazz never
 * prints stale setup instructions. There is deliberately no bundled fallback:
 * `npm create jazz` needs network access. The URL is unversioned on purpose,
 * which makes it a prompt surface that jazz.tools controls for every
 * create-jazz version: whatever it serves is what coding agents are told.
 */
export const BRIEF_URL = "https://jazz.tools/quickstart.md";

const FETCH_TIMEOUT_MS = 10_000;
/** A brief is a page of Markdown; anything far larger is not one. */
export const MAX_BRIEF_BYTES = 256 * 1024;

const TTY_HINT =
  "Paste this into your coding agent, or tell it to run `npm create jazz` and follow the output.";

export async function fetchBrief(url: string, fetchImpl: typeof fetch = fetch): Promise<string> {
  const res = await fetchImpl(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  // A misrouted URL tends to answer with an HTML page; never print that as the brief.
  const contentType = res.headers.get("content-type") ?? "";
  if (contentType.includes("text/html")) throw new Error(`unexpected content-type ${contentType}`);
  const text = await res.text();
  if (new TextEncoder().encode(text).byteLength > MAX_BRIEF_BYTES) {
    throw new Error(`response larger than ${MAX_BRIEF_BYTES} bytes`);
  }
  const brief = text.trim();
  if (!brief) throw new Error("empty response");
  return brief;
}

/** What goes to stdout. Humans get one hint line; agents (no TTY) get only the brief. */
export function renderBrief(brief: string, isTTY: boolean): string {
  return isTTY ? `${TTY_HINT}\n\n${brief}\n` : `${brief}\n`;
}

export function renderFetchError(url: string, err: unknown): string {
  const reason = err instanceof Error ? err.message : String(err);
  return `Could not fetch the Jazz agent brief from ${url} (${reason}).\nFetch ${url} directly and follow it.\n`;
}
