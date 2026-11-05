// Vercel Web Analytics custom events for docs searches, shared by the site's
// search box (client) and the Markdown search endpoint for agents (server).
//
// Vercel Pro allows two properties per custom event, so the event carries the
// query and where it came from, and searches without results get their own
// event name, which keeps docs gaps one filter away in the dashboard.

export type SearchSource = "ui" | "api";

export const SEARCH_EVENT = "Docs search";
export const SEARCH_NO_RESULTS_EVENT = "Docs search without results";

/** Vercel caps event names, keys and values at 255 characters. */
const MAX_VALUE_CHARS = 255;

/** Shortest query worth recording; single letters are mostly typing in progress. */
export const MIN_TRACKED_QUERY_CHARS = 2;

export function searchEvent(
  query: string,
  source: SearchSource,
  resultCount: number,
): { name: string; properties: { query: string; source: SearchSource } } | null {
  const normalized = query.trim().replace(/\s+/g, " ").toLowerCase();
  if (normalized.length < MIN_TRACKED_QUERY_CHARS) return null;
  return {
    name: resultCount === 0 ? SEARCH_NO_RESULTS_EVENT : SEARCH_EVENT,
    properties: { query: normalized.slice(0, MAX_VALUE_CHARS), source },
  };
}
