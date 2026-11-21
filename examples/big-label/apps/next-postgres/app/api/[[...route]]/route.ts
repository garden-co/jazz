import { handle } from "hono/vercel";
import { api } from "../../../src/lib/api.server";

export const runtime = "nodejs";
export const GET = handle(api);
export const POST = handle(api);
