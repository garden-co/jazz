import type { Timeline } from "./model";

// One /api/timeline request per page view, shared by the example metrics and
// the full history explorer. A forced refresh replaces the shared promise.
let pending: Promise<Timeline> | null = null;

export function fetchTimeline(force = false): Promise<Timeline> {
  if (!pending || force) {
    const request = fetch("/api/timeline").then(async (response) => {
      if (!response.ok)
        throw new Error("CodSpeed history is temporarily unavailable. Please try again.");
      return (await response.json()) as Timeline;
    });
    // A failed request must not poison later callers.
    request.catch(() => {
      if (pending === request) pending = null;
    });
    pending = request;
  }
  return pending;
}
