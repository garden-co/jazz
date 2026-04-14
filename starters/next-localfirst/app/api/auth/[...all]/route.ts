import { NextResponse } from "next/server";
import { toNextJsHandler } from "better-auth/next-js";

if (process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH === "1") {
  // Eagerly import auth so the module is ready — only when flag is on.
  // (Dynamic import used to avoid loading BetterAuth when flag is off.)
}

export async function GET(req: Request) {
  if (process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH !== "1") {
    return new NextResponse("Not Found", { status: 404 });
  }
  const { auth } = await import("@/src/lib/auth");
  return toNextJsHandler(auth.handler).GET(req as any);
}

export async function POST(req: Request) {
  if (process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH !== "1") {
    return new NextResponse("Not Found", { status: 404 });
  }
  const { auth } = await import("@/src/lib/auth");
  return toNextJsHandler(auth.handler).POST(req as any);
}
