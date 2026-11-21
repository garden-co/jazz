import { mustGetQuery } from "@rocicorp/zero";
import { handleQueryRequest } from "@rocicorp/zero/server";
import { queries } from "../../../../queries";
import { schema } from "../../../../schema";
import { authData } from "../../../../src/lib/zero.server";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const ctx = await authData(request);
  if (!ctx) return Response.json({ error: "Unauthorized" }, { status: 401 });
  const result = await handleQueryRequest({
    handler: (name, args) => mustGetQuery(queries, name).fn({ args, ctx }),
    schema,
    request,
    userID: ctx.userID,
  });
  return Response.json(result);
}
