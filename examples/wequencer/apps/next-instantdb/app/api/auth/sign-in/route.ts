import { authRoute, signIn } from "@/lib/instant.server";

export const runtime = "nodejs";
export const POST = authRoute(signIn);
