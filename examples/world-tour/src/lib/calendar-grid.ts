export interface CalendarDay {
  date: Date;
  dayOfMonth: number;
  isCurrentMonth: boolean;
}

/**
 * Build a month grid for the given year/month (0-indexed month).
 * Weeks start on Monday (ISO). Pads first week with previous-month
 * days and last week with next-month days.
 */
export function buildMonthGrid(year: number, month: number): CalendarDay[][] {
  const firstOfMonth = new Date(year, month, 1);

  // getDay() returns 0=Sun..6=Sat; convert to Mon=0..Sun=6
  const dayOfWeek = (firstOfMonth.getDay() + 6) % 7;

  // Start from the Monday of the first week
  const gridStart = new Date(year, month, 1 - dayOfWeek);

  const weeks: CalendarDay[][] = [];
  const cursor = new Date(gridStart);

  // Generate weeks until we've passed the end of the month
  // and completed the current week row
  while (true) {
    const week: CalendarDay[] = [];
    for (let d = 0; d < 7; d++) {
      week.push({
        date: new Date(cursor),
        dayOfMonth: cursor.getDate(),
        isCurrentMonth: cursor.getMonth() === month && cursor.getFullYear() === year,
      });
      cursor.setDate(cursor.getDate() + 1);
    }
    weeks.push(week);

    // Stop once we've entered the next month and finished the week
    if (cursor.getMonth() !== month || cursor.getFullYear() !== year) {
      // We've moved past the target month — check if the last week
      // we just pushed already contains days from the next month.
      // If so, we're done.
      if (weeks.length >= 4) break;
    }
  }

  return weeks;
}

function formatDateKey(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

/**
 * Map stops onto the grid, grouping by YYYY-MM-DD date string.
 * Only includes stops whose date matches a cell in the grid.
 */
export function mapStopsToGrid(
  stops: Array<{ id: string; date: Date }>,
  grid: CalendarDay[][],
): Map<string, Array<{ id: string }>> {
  // Build a set of all date keys present in the grid
  const gridDates = new Set<string>();
  for (const week of grid) {
    for (const day of week) {
      gridDates.add(formatDateKey(day.date));
    }
  }

  const result = new Map<string, Array<{ id: string }>>();

  for (const stop of stops) {
    const key = formatDateKey(stop.date);
    if (!gridDates.has(key)) continue;

    let bucket = result.get(key);
    if (!bucket) {
      bucket = [];
      result.set(key, bucket);
    }
    bucket.push({ id: stop.id });
  }

  return result;
}
