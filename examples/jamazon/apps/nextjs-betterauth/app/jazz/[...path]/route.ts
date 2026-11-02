import { backend } from "@/src/server/backend";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// Sync and sign-in for the browser, on the store's own origin.
export async function GET(request: Request) {
  return (await backend()).next().GET(request);
}
export async function POST(request: Request) {
  return (await backend()).next().POST(request);
}
export async function DELETE(request: Request) {
  return (await backend()).next().DELETE(request);
}
