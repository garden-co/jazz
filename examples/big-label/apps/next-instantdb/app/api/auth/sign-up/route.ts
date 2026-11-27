import { authRoute, signUp } from "../../../../src/lib/instant.server";

export const runtime = "nodejs";
export const POST = authRoute(signUp);
