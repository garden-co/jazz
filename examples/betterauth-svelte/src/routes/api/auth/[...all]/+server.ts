import { auth } from "$lib/auth";
import type { RequestHandler } from "./$types";

export const GET: RequestHandler = async (event) => {
  return await auth.handler(event.request);
};

export const POST: RequestHandler = async (event) => {
  return await auth.handler(event.request);
};

