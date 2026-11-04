import type { Db } from "jazz-tools";
import { app } from "@/schema";

/** What a tool may read: the workspace's own seeded booking data. */
export type ToolContext = {
  db: Db;
  ownerAccount: string;
  artistId: string;
  today: string; // ISO date
};

type JsonSchema = {
  type: "object";
  properties: Record<string, unknown>;
  required: string[];
  additionalProperties: false;
};

export type ToolDefinition = {
  name: ToolName;
  description: string;
  input_schema: JsonSchema;
};

export type ToolName = "find_venues" | "check_calendar" | "draft_setlist";

export const toolDefinitions: ToolDefinition[] = [
  {
    name: "find_venues",
    description:
      "Search the booking agency's venue list. Returns rooms with capacity, style and booking contact. Filter by city and capacity.",
    input_schema: {
      type: "object",
      properties: {
        city: { type: "string", description: "City name, e.g. Chicago. Omit for all cities." },
        min_capacity: { type: "integer", description: "Smallest acceptable capacity." },
        max_capacity: { type: "integer", description: "Largest acceptable capacity." },
      },
      required: [],
      additionalProperties: false,
    },
  },
  {
    name: "check_calendar",
    description:
      "List the artist's shows, holds, travel and studio days between two dates (inclusive), plus the free dates in that range. Dates are YYYY-MM-DD; at most 62 days.",
    input_schema: {
      type: "object",
      properties: {
        from: { type: "string", description: "First date, YYYY-MM-DD." },
        to: { type: "string", description: "Last date, YYYY-MM-DD." },
      },
      required: ["from", "to"],
      additionalProperties: false,
    },
  },
  {
    name: "draft_setlist",
    description:
      "Draft a setlist from the artist's catalogue that fits a slot length, ordered for the requested mood.",
    input_schema: {
      type: "object",
      properties: {
        minutes: { type: "integer", description: "Slot length in minutes, 15 to 120." },
        mood: { type: "string", enum: ["intimate", "balanced", "high-energy"] },
      },
      required: ["minutes"],
      additionalProperties: false,
    },
  },
];

export function isToolName(name: string): name is ToolName {
  return toolDefinitions.some((tool) => tool.name === name);
}

/** Validate model-supplied input before running a tool; throws a readable error. */
export function parseToolInput(name: ToolName, input: unknown): Record<string, unknown> {
  if (typeof input !== "object" || input === null || Array.isArray(input))
    throw new Error(`${name} expects an object`);
  const record = input as Record<string, unknown>;
  const definition = toolDefinitions.find((tool) => tool.name === name)!;
  for (const key of definition.input_schema.required)
    if (!(key in record)) throw new Error(`${name} is missing "${key}"`);
  for (const key of Object.keys(record))
    if (!(key in definition.input_schema.properties))
      throw new Error(`${name} does not take "${key}"`);
  return record;
}

export async function runTool(
  context: ToolContext,
  name: ToolName,
  input: Record<string, unknown>,
) {
  switch (name) {
    case "find_venues":
      return findVenues(context, input);
    case "check_calendar":
      return checkCalendar(context, input);
    case "draft_setlist":
      return draftSetlist(context, input);
  }
}

async function findVenues({ db, ownerAccount }: ToolContext, input: Record<string, unknown>) {
  const city = typeof input.city === "string" ? input.city.trim().toLowerCase() : undefined;
  const min = typeof input.min_capacity === "number" ? input.min_capacity : 0;
  const max = typeof input.max_capacity === "number" ? input.max_capacity : Infinity;
  const venues = await db.all(app.venues.where({ ownerAccount }));
  return {
    venues: venues
      .filter((venue) => !city || venue.city.toLowerCase() === city)
      .filter((venue) => venue.capacity >= min && venue.capacity <= max)
      .sort((a, b) => a.capacity - b.capacity)
      .map(({ name, city, capacity, style, bookingContact }) => ({
        name,
        city,
        capacity,
        style,
        bookingContact,
      })),
  };
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

export function addDays(isoDate: string, days: number): string {
  const date = new Date(`${isoDate}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

async function checkCalendar(
  { db, ownerAccount, artistId }: ToolContext,
  input: Record<string, unknown>,
) {
  const from = String(input.from);
  const to = String(input.to);
  if (!ISO_DATE.test(from) || !ISO_DATE.test(to)) throw new Error("dates must be YYYY-MM-DD");
  if (to < from) throw new Error("`to` is before `from`");
  if (addDays(from, 62) < to) throw new Error("range is longer than 62 days");
  const events = (await db.all(app.calendarEvents.where({ ownerAccount, artistId })))
    .filter((event) => event.date >= from && event.date <= to)
    .sort((a, b) => a.date.localeCompare(b.date));
  const busy = new Set(events.map((event) => event.date));
  const freeDates: string[] = [];
  for (let date = from; date <= to; date = addDays(date, 1))
    if (!busy.has(date)) freeDates.push(date);
  return {
    events: events.map(({ date, kind, title, city }) => ({ date, kind, title, city })),
    freeDates,
  };
}

async function draftSetlist(
  { db, ownerAccount, artistId }: ToolContext,
  input: Record<string, unknown>,
) {
  const minutes = Math.min(120, Math.max(15, Math.round(Number(input.minutes) || 45)));
  const mood = input.mood === "intimate" || input.mood === "high-energy" ? input.mood : "balanced";
  const songs = await db.all(app.songs.where({ ownerAccount, artistId }));
  const rank = { low: 0, medium: 1, high: 2 } as const;
  // Intimate sets favour quiet songs, high-energy sets loud ones; a balanced
  // set keeps catalogue order. Stable sort keeps the draft deterministic.
  const preferred = [...songs].sort((a, b) =>
    mood === "balanced"
      ? a.title.localeCompare(b.title)
      : mood === "intimate"
        ? rank[a.energy] - rank[b.energy] || a.title.localeCompare(b.title)
        : rank[b.energy] - rank[a.energy] || a.title.localeCompare(b.title),
  );
  const picked: typeof songs = [];
  let seconds = 0;
  for (const song of preferred) {
    if (seconds + song.durationSeconds > minutes * 60) continue;
    picked.push(song);
    seconds += song.durationSeconds;
  }
  // Open mid-tempo, close on the strongest song.
  picked.sort((a, b) => rank[a.energy] - rank[b.energy]);
  const opener = picked.findIndex((song) => song.energy === "medium");
  if (opener > 0) picked.unshift(...picked.splice(opener, 1));
  return {
    mood,
    targetMinutes: minutes,
    totalMinutes: Math.round(seconds / 6) / 10,
    songs: picked.map((song, index) => ({
      position: index + 1,
      title: song.title,
      minutes: Math.round(song.durationSeconds / 6) / 10,
      energy: song.energy,
    })),
  };
}
