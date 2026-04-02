import { describe, it, expect } from "vitest";
import { buildMonthGrid, mapStopsToGrid, type CalendarDay } from "../calendar-grid.js";

describe("buildMonthGrid", () => {
  it("starts on June 1 (Monday) when the month begins on Monday", () => {
    // June 2026 starts on a Monday
    const grid = buildMonthGrid(2026, 5); // month is 0-indexed

    const firstDay = grid[0]![0]!;
    expect(firstDay.date.getFullYear()).toBe(2026);
    expect(firstDay.date.getMonth()).toBe(5);
    expect(firstDay.date.getDate()).toBe(1);
    expect(firstDay.dayOfMonth).toBe(1);
    expect(firstDay.isCurrentMonth).toBe(true);
  });

  it("pads the first week with previous-month days when the month starts mid-week", () => {
    // July 2026 starts on Wednesday
    // Grid should start on Monday Jun 29
    const grid = buildMonthGrid(2026, 6);

    const mon = grid[0]![0]!;
    expect(mon.date.getFullYear()).toBe(2026);
    expect(mon.date.getMonth()).toBe(5); // June
    expect(mon.date.getDate()).toBe(29);
    expect(mon.dayOfMonth).toBe(29);
    expect(mon.isCurrentMonth).toBe(false);

    const tue = grid[0]![1]!;
    expect(tue.date.getMonth()).toBe(5); // June
    expect(tue.date.getDate()).toBe(30);
    expect(tue.isCurrentMonth).toBe(false);

    // Wednesday Jul 1 is the first current-month day
    const wed = grid[0]![2]!;
    expect(wed.date.getMonth()).toBe(6); // July
    expect(wed.date.getDate()).toBe(1);
    expect(wed.isCurrentMonth).toBe(true);
  });

  it("includes all 29 days of February in a leap year", () => {
    // 2028 is a leap year
    const grid = buildMonthGrid(2028, 1);

    const allDays = grid.flat();
    const febDays = allDays.filter((d) => d.date.getMonth() === 1 && d.date.getFullYear() === 2028);

    expect(febDays).toHaveLength(29);
    expect(febDays[febDays.length - 1]!.dayOfMonth).toBe(29);
  });

  it("returns rows of exactly 7 days each", () => {
    // Check a few different months
    for (const [year, month] of [
      [2026, 5],
      [2026, 6],
      [2028, 1],
      [2026, 0],
    ] as const) {
      const grid = buildMonthGrid(year, month);

      expect(grid.length).toBeGreaterThanOrEqual(4);
      expect(grid.length).toBeLessThanOrEqual(6);

      for (const week of grid) {
        expect(week).toHaveLength(7);
      }
    }
  });

  it("marks days outside the current month as isCurrentMonth: false", () => {
    // July 2026 — first row has trailing June days, last row has leading August days
    const grid = buildMonthGrid(2026, 6);

    const allDays = grid.flat();
    for (const day of allDays) {
      if (day.date.getMonth() === 6 && day.date.getFullYear() === 2026) {
        expect(day.isCurrentMonth).toBe(true);
      } else {
        expect(day.isCurrentMonth).toBe(false);
      }
    }
  });
});

describe("mapStopsToGrid", () => {
  // Helper: build a grid for June 2026 once
  function juneGrid(): CalendarDay[][] {
    return buildMonthGrid(2026, 5);
  }

  it("places a stop on the correct date", () => {
    const grid = juneGrid();
    const stops = [{ id: "london", date: new Date("2026-06-05") }];

    const result = mapStopsToGrid(stops, grid);

    expect(result.get("2026-06-05")).toEqual([{ id: "london" }]);
  });

  it("groups multiple stops on the same date", () => {
    const grid = juneGrid();
    const stops = [
      { id: "morning-gig", date: new Date("2026-06-12") },
      { id: "evening-gig", date: new Date("2026-06-12") },
    ];

    const result = mapStopsToGrid(stops, grid);

    const entries = result.get("2026-06-12");
    expect(entries).toHaveLength(2);
    expect(entries).toEqual(expect.arrayContaining([{ id: "morning-gig" }, { id: "evening-gig" }]));
  });

  it("ignores stops that fall outside the grid date range", () => {
    const grid = juneGrid();
    // A stop far outside June 2026's grid
    const stops = [{ id: "off-grid", date: new Date("2027-01-15") }];

    const result = mapStopsToGrid(stops, grid);

    expect(result.get("2027-01-15")).toBeUndefined();
  });
});
