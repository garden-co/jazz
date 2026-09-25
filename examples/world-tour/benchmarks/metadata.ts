import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/world-tour/benchmarks/benches/walltime.rs";

export const worldTourBenchmarks: BenchmarkMetadata[] = [];
for (const stops of [128, 4096]) {
  const fixture = `One band with ${stops.toLocaleString("en-US")} dated tour stops, each at its own venue. 22 stops fall inside the three-week window; about 1 in 5 are tentative and 1 in 7 cancelled.`;
  const shared = {
    fixture,
    storage: "In-memory Jazz database",
    excludes: [
      "Schema compilation, database opening and seeding",
      "Query preparation",
      "Map rendering, sync and network latency",
    ],
    source,
  };
  worldTourBenchmarks.push(
    {
      ...shared,
      name: `world_tour_member_calendar_window[${stops}]`,
      title: "WorldTour · band member's calendar",
      description:
        "Open the next three weeks of the tour as a band member: the first 12 stops in date order, each with its venue. Today the cost grows with the whole table, not just the page (#1962).",
      includes: ["One prepared read: date range, date order, limit 12, venue included"],
      work: {
        count: 1,
        unit: "calendar views/s",
        explanation:
          "One calendar window of at most 12 stops per iteration. The window holds 22 stops at every scale, but the cost currently grows with the total stop count (#1962).",
      },
    },
    {
      ...shared,
      name: `world_tour_public_calendar_window[${stops}]`,
      title: "WorldTour · public calendar",
      description:
        "Open the same three weeks as a fan: only confirmed stops, first 12 in date order, each with its venue. Today the cost grows with the whole table, not just the page (#1962).",
      includes: [
        "One prepared read: confirmed filter, date range, date order, limit 12, venue included",
      ],
      work: {
        count: 1,
        unit: "calendar views/s",
        explanation:
          "One calendar window of at most 12 stops per iteration. The window holds 22 stops at every scale, but the cost currently grows with the total stop count (#1962).",
      },
    },
  );
}
