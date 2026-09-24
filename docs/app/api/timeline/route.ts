import { loadTimeline } from "@/lib/perf-timeline/source";
import type { Timeline } from "@/lib/perf-timeline/model";

export const maxDuration = 60;

// Last timeline this instance built successfully. It is per server instance,
// so a cold instance still returns 502 when CodSpeed fails; the CDN's
// stale-while-revalidate window covers most of that case.
let lastGood: Timeline | null = null;

export async function GET() {
  try {
    lastGood = await loadTimeline();
    return Response.json(lastGood, {
      headers: { "Cache-Control": "public, s-maxage=1800, stale-while-revalidate=86400" },
    });
  } catch (error) {
    console.error(
      "Timeline upstream fetch failed",
      error instanceof Error ? error.message : "unknown",
    );
    if (lastGood) {
      return Response.json(
        {
          ...lastGood,
          warnings: [
            `CodSpeed is unavailable right now; showing history retrieved ${lastGood.fetchedAt}.`,
            ...lastGood.warnings,
          ],
        },
        // Short, so a recovered CodSpeed replaces the fallback quickly.
        { headers: { "Cache-Control": "public, s-maxage=60" } },
      );
    }
    return Response.json(
      { error: "Benchmark history is temporarily unavailable. Try again in a moment." },
      { status: 502 },
    );
  }
}
