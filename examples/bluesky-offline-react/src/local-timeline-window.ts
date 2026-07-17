export const timelinePageSize = 20;
export const initialTimelineLimit = timelinePageSize;

export function timelineQueryLimit(visibleLimit: number) {
  return visibleLimit + 1;
}

export function nextTimelineLimit(currentLimit: number) {
  return currentLimit + timelinePageSize;
}

export function windowTimelineRows<Row>(rows: Row[], visibleLimit: number) {
  return {
    rows: rows.slice(0, visibleLimit),
    hasMore: rows.length > visibleLimit,
  };
}
