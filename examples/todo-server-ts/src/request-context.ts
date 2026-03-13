import type { Request } from "express";
import { type JazzContext, type Session } from "jazz-tools/backend";
import { app as schemaApp } from "../schema/app.js";

type RequesterIdentity = {
  userId: string;
  claims: Record<string, unknown>;
};

function verifyJwtAndExtractIdentity(_jwt: string): RequesterIdentity {
  // Replace with your auth provider's JWT verification logic.
  return { userId: "replace-with-verified-sub", claims: {} };
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
  const userDb = context.forSession(sessionFromRequest(req), schemaApp);
  const rows = await userDb.all(schemaApp.todos);
  return rows;
}

export async function listTodosWithSimplePolicy(req: Request, context: JazzContext) {
  const userDb = context.forSession(sessionFromRequest(req), schemaApp);
  return userDb.all(schemaApp.todos);
}

export async function listTodosWithInheritedPolicy(
  req: Request,
  context: JazzContext,
  folderId: string,
) {
  const userDb = context.forSession(sessionFromRequest(req), schemaApp);
  return userDb.all(schemaApp.todos.where({ project: folderId }));
}
