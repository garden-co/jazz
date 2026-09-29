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
