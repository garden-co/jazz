import type { StopStatus } from "../../schema.js";

const long = new Intl.DateTimeFormat("en-GB", {
  weekday: "long",
  day: "numeric",
  month: "long",
  year: "numeric",
});
const short = new Intl.DateTimeFormat("en-GB", {
  weekday: "short",
  day: "numeric",
  month: "short",
});

export const formatLongDate = (date: Date) => long.format(date);
export const formatShortDate = (date: Date) => short.format(date);

/** `YYYY-MM-DD` in local time, for `<input type="date">`. */
export function toDateInput(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** Parses `YYYY-MM-DD` as local midnight. */
export function fromDateInput(value: string): Date {
  const [y, m, d] = value.split("-").map(Number);
  return new Date(y, m - 1, d);
}

export const statusLabels: Record<StopStatus, string> = {
  confirmed: "Confirmed",
  tentative: "Tentative",
  cancelled: "Cancelled",
};
