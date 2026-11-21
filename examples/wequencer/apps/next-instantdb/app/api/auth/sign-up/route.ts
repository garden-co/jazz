import { authRoute, signUp } from "@/lib/instant.server";

export const runtime = "nodejs";
export const POST = authRoute(signUp);
