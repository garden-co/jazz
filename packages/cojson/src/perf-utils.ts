import { RawCoID } from "./exports";

export const performanceMarks = {
  loadFromStorageStart: "cojson.load_from_storage.start",
  loadFromStorageEnd: "cojson.load_from_storage.end",
  loadFromPeerStart: "cojson.load_from_peer.start",
  loadFromPeerEnd: "cojson.load_from_peer.end",
} as const;

const performanceMarkAvailable =
  "mark" in performance && "getEntriesByName" in performance;

export function trackPerformanceMark(
  mark: keyof typeof performanceMarks,
  coId: RawCoID,
  detail?: Record<string, any>,
) {
  if (!performanceMarkAvailable) {
    return;
  }

  performance.mark(performanceMarks[mark] + "." + coId, {
    detail,
  });
}
