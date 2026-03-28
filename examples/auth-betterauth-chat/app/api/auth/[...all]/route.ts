import { toNextJsHandler } from "better-auth/next-js";
import { auth } from "../../../../src/lib/auth";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

async function handler(request: Request) {
  return (await auth).handler(request);
}

export const { GET, POST, PATCH, PUT, DELETE } = toNextJsHandler(handler);
