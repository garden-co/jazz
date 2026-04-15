import type { CodexCompletionEvent, CodexSessionProjection } from "./store.js";
import { completionEventsFromProjection } from "./projector.js";

const MAX_EMITTED_IDS = 512;
const RETAINED_EMITTED_IDS = 320;

export function collectRecentCompletionEvents(options: {
  projection: CodexSessionProjection;
  previousMtime?: number;
  bootstrapCutoff: number;
  emittedIds: ReadonlySet<string>;
}): CodexCompletionEvent[] {
  const { projection, previousMtime, bootstrapCutoff, emittedIds } = options;

  return completionEventsFromProjection(projection).filter((event) => {
    if (emittedIds.has(event.id)) {
      return false;
    }

    if (previousMtime !== undefined) {
      return event.completedAt.getTime() >= previousMtime;
    }

    return event.completedAt.getTime() >= bootstrapCutoff;
  });
}

export function trackEmittedId(ids: Set<string>, order: string[], id: string): void {
  ids.add(id);
  order.push(id);
  if (order.length <= MAX_EMITTED_IDS) {
    return;
  }

  const staleIds = order.splice(0, order.length - RETAINED_EMITTED_IDS);
  for (const staleId of staleIds) {
    ids.delete(staleId);
  }
}
