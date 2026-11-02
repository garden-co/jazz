import { jazz } from "../../../src/lib/jazz.server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// Sync and sign-in for the browser, on this app's own origin.
export async function GET(request: Request) {
  return (await jazz()).next().GET(request);
}
export async function POST(request: Request) {
  return (await jazz()).next().POST(request);
}
export async function DELETE(request: Request) {
  return (await jazz()).next().DELETE(request);
}
