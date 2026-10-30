import { publicCatalogue } from "@/src/server/public-catalogue";

export const runtime = "nodejs";
// Read per request; the Cache-Control header below lets a CDN share it.
export const dynamic = "force-dynamic";

/**
 * The public catalogue (categories, products, stock levels) for a shopper's
 * first paint on a catalogue page, fetched by the browser while its own Jazz
 * client opens and syncs. Only the catalogue pages ask for it, and it never
 * holds up a page's HTML: a slow or stalled sync server only means the page
 * waits for its own sync, as it did before.
 */
export async function GET() {
  const snapshot = await publicCatalogue();
  if (!snapshot)
    return Response.json(
      { error: "catalogue unavailable" },
      {
        status: 503,
        headers: { "cache-control": "no-store" },
      },
    );
  return Response.json(snapshot, {
    headers: { "cache-control": "public, max-age=0, s-maxage=30, stale-while-revalidate=300" },
  });
}
