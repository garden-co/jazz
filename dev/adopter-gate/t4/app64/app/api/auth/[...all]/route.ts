import { toNextJsHandler } from "better-auth/next-js";
import { auth } from "@/jazz.server";

export const { GET, POST } = toNextJsHandler(auth);
