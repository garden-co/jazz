import type { Request } from "express";
import { translateQuery, type JazzContext, type Session } from "jazz-tools/backend";
import { app as schemaApp, wasmSchema as schema } from "../schema/app.js";

type RequesterIdentity = {
  userId: string;
  claims: Record<string, unknown>;
};

function verifyJwtAndExtractIdentity(_jwt: string): RequesterIdentity {
  // Replace with your auth provider's JWT verification logic.
  return { userId: "replace-with-verified-sub", claims: {} };
}

function buildQuery(table: string): string {
  return translateQuery(
    JSON.stringify({
      table,
      conditions: [],
      includes: {},
      orderBy: [],
    }),
    schema,
  );
}

function buildFolderScopedQuery(folderId: string): string {
  return translateQuery(
    JSON.stringify({
      table: "todos",
      conditions: [{ column: "folder_id", op: "eq", value: folderId }],
      includes: {},
      orderBy: [],
    }),
    schema,
  );
}

export function sessionFromRequest(req: Request): Session {
  const auth = req.header("authorization");
  if (!auth?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const jwt = auth.slice("Bearer ".length);
  const identity = verifyJwtAndExtractIdentity(jwt);
  return { user_id: identity.userId, claims: identity.claims };
}

export async function listTodosForRequester(req: Request, context: JazzContext) {
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  const rows = await userClient.query(buildQuery("todos"));
  return rows;
}

export async function listTodosWithSimplePolicy(req: Request, context: JazzContext) {
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  return userClient.query(buildQuery("todos"));
}

export async function listTodosWithInheritedPolicy(
  req: Request,
  context: JazzContext,
  folderId: string,
) {
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  return userClient.query(buildFolderScopedQuery(folderId));
}
