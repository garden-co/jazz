import { auth } from "~/server/utils/auth";
import { toNodeHandler } from "better-auth/node";

const handler = toNodeHandler(auth);

export default defineEventHandler((event) => {
  return handler(event.node.req, event.node.res);
});
