import { loadTimeline } from "@/lib/perf-timeline/source";
import type { Timeline } from "@/lib/perf-timeline/model";

export const dynamic = "force-dynamic";
export const maxDuration = 30;

// Last snapshot this instance read successfully. It is per server instance,
// so a cold instance still returns 502 when the snapshot cannot be read; the
// CDN's stale-while-revalidate window covers most of that case.
let lastGood: Timeline | null = null;

export async function GET() {
  try {
    lastGood = await loadTimeline();
    return Response.json(lastGood, {
      headers: { "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=604800" },
    });
  } catch (error) {
    console.error(
      "Timeline snapshot fetch failed",
      error instanceof Error ? error.message : "unknown",
    );
    if (lastGood) {
      return Response.json(lastGood, { headers: { "Cache-Control": "public, s-maxage=60" } });
    }
    return Response.json(
      { error: "Benchmark history is temporarily unavailable. Try again in a moment." },
      { status: 502, headers: { "Cache-Control": "public, s-maxage=60" } },
    );
  }
}
