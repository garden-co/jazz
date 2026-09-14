import { loadTimeline } from "@/lib/perf-timeline/source";

export const maxDuration = 60;
export async function GET() {
  try {
    return Response.json(await loadTimeline(), {
      headers: { "Cache-Control": "public, s-maxage=300, stale-while-revalidate=600" },
    });
  } catch (error) {
    console.error(
      "Timeline upstream fetch failed",
      error instanceof Error ? error.message : "unknown",
    );
    return Response.json(
      { error: "Benchmark history is temporarily unavailable. Try again in a moment." },
      { status: 502 },
    );
  }
}
