import { mustGetMutator } from "@rocicorp/zero";
import { handleMutateRequest } from "@rocicorp/zero/server";
import { authData, dbProvider } from "@/lib/zero.server";
import { mutators } from "@/mutators";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const ctx = await authData(request);
  if (!ctx) return Response.json({ error: "Unauthorized" }, { status: 401 });
  const result = await handleMutateRequest({
    dbProvider,
    handler: (transact) =>
      transact((tx, name, args) => mustGetMutator(mutators, name).fn({ tx, args, ctx })),
    request,
    userID: ctx.userID,
  });
  return Response.json(result);
}
