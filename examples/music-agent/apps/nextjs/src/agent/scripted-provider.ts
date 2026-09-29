import type { AgentProvider, TurnSink } from "./provider";
import { addDays } from "./tools";

const CITIES = ["Chicago", "Detroit", "Milwaukee", "Minneapolis", "Toronto"];

type Venue = {
  name: string;
  city: string;
  capacity: number;
  style: string;
  bookingContact: string;
};
type Calendar = {
  events: { date: string; kind: string; title: string; city: string }[];
  freeDates: string[];
};
type Setlist = {
  mood: string;
  totalMinutes: number;
  songs: { position: number; title: string; minutes: number }[];
};

/**
 * A deterministic stand-in for a model. It reads the latest request, calls the
 * same tools a model would, and streams a reply built from their results word
 * by word. The same conversation always produces the same reply, which makes
 * demos, tests and interrupted-turn replay reproducible without an API key.
 */
export function scriptedProvider(tokenDelayMs: number): AgentProvider {
  return {
    id: "scripted",
    label: "Scripted agent",
    resume: "replay",
    async generate(input, sink) {
      const say = (text: string) => stream(sink, text, tokenDelayMs);
      const request = input.history.at(-1);
      const prompt = request?.text.toLowerCase() ?? "";
      let answered = false;

      const clip = request?.attachments.find((file) => file.mediaType.startsWith("audio/"));
      if (clip) {
        await say(
          `Thanks for the clip. I've filed **${clip.filename}** (${formatBytes(clip.byteLength)}) with this conversation so promoters can hear it from the pitch.\n\n`,
        );
      }

      if (/venue|room|club|hall|where|play in/.test(prompt)) {
        answered = true;
        const city = CITIES.find((name) => prompt.includes(name.toLowerCase()));
        const { venues } = (await sink.tool("find_venues", city ? { city } : {})) as {
          venues: Venue[];
        };
        await say(
          venues.length
            ? `Here are the rooms I'd pitch${city ? ` in ${city}` : ""}:\n\n${venues
                .slice(0, 4)
                .map(
                  (venue) =>
                    `- **${venue.name}**, ${venue.city}: ${venue.capacity} cap, ${venue.style}. Booking: ${venue.bookingContact}`,
                )
                .join("\n")}\n\n`
            : `I don't have any rooms on file${city ? ` in ${city}` : ""} yet.\n\n`,
        );
      }

      if (/free|available|calendar|weekend|date|when|schedule/.test(prompt)) {
        answered = true;
        const from = input.tools.today;
        const to = addDays(from, 27);
        const { events, freeDates } = (await sink.tool("check_calendar", { from, to })) as Calendar;
        const weekends = freeDates.filter((date) =>
          [5, 6].includes(new Date(`${date}T00:00:00Z`).getUTCDay()),
        );
        await say(
          `Over the next four weeks the calendar has ${events.length} commitments${
            events.length
              ? `, starting with ${events[0]!.title} on ${formatDate(events[0]!.date)}`
              : ""
          }. ${
            weekends.length
              ? `Open Friday and Saturday nights: ${weekends.slice(0, 4).map(formatDate).join(", ")}.`
              : "There are no open weekend nights, so I'd look at a midweek slot."
          }\n\n`,
        );
      }

      if (/setlist|set list|songs|\bset\b/.test(prompt)) {
        answered = true;
        const minutes = Number(/(\d{2,3})\s*(?:min|minute)/.exec(prompt)?.[1] ?? 45);
        const mood = /intimate|quiet|acoustic/.test(prompt)
          ? "intimate"
          : /party|loud|energy|festival/.test(prompt)
            ? "high-energy"
            : "balanced";
        const setlist = (await sink.tool("draft_setlist", { minutes, mood })) as Setlist;
        await say(
          `A ${setlist.mood} set that runs ${setlist.totalMinutes} minutes:\n\n${setlist.songs
            .map((song) => `${song.position}. ${song.title} (${song.minutes} min)`)
            .join("\n")}\n\n`,
        );
      }

      if (!answered && !clip) {
        await say(
          `I'm the booking desk for ${input.artistName}. Ask me to find venues in a city, check which dates are free, or draft a setlist for a slot, and attach a rough mix when you want it in the pitch.\n\n`,
        );
      }
      await say(
        answered ? "Want me to draft the pitch email next?" : "What should we work on first?",
      );
    },
  };
}

/** Stream prose in word-sized pieces so it arrives the way model output does. */
async function stream(sink: TurnSink, text: string, delayMs: number) {
  for (const piece of text.match(/\s*\S+|\s+/g) ?? []) {
    await sink.text(piece);
    if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

function formatDate(isoDate: string) {
  return new Date(`${isoDate}T00:00:00Z`).toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
}

function formatBytes(bytes: number) {
  return bytes >= 1_000_000
    ? `${(bytes / 1_000_000).toFixed(1)} MB`
    : `${Math.round(bytes / 1000)} kB`;
}
