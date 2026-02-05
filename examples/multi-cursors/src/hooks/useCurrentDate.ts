import { useState, useEffect } from "react";

/**
 * Returns the current date as a string in YYYY-MM-DD format.
 */
function getTodayDateString(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

/**
 * Returns the number of milliseconds until midnight.
 */
function getMsUntilMidnight(): number {
  const now = new Date();
  const midnight = new Date(now);
  midnight.setHours(24, 0, 0, 0);
  return midnight.getTime() - now.getTime();
}

/**
 * A hook that returns the current date as a reactive string (YYYY-MM-DD).
 * Automatically updates when the day changes at midnight.
 */
export function useCurrentDate(): string {
  const [date, setDate] = useState(getTodayDateString);

  useEffect(() => {
    let timeoutId: ReturnType<typeof setTimeout>;

    function scheduleNextUpdate() {
      const msUntilMidnight = getMsUntilMidnight();
      // Add a small buffer (100ms) to ensure we're past midnight
      timeoutId = setTimeout(() => {
        setDate(getTodayDateString());
        scheduleNextUpdate();
      }, msUntilMidnight + 100);
    }

    scheduleNextUpdate();

    return () => clearTimeout(timeoutId);
  }, []);

  return date;
}
