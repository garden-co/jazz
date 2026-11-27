import { authRoute, signIn } from "../../../../src/lib/instant.server";

export const runtime = "nodejs";
export const POST = authRoute(signIn);
