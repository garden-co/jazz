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
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  const rows = await userClient.query(schemaApp.todos);
  return rows;
}

export async function listTodosWithSimplePolicy(req: Request, context: JazzContext) {
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  return userClient.query(schemaApp.todos);
}

export async function listTodosWithInheritedPolicy(
  req: Request,
  context: JazzContext,
  folderId: string,
) {
  const userClient = context.forSession(sessionFromRequest(req), schemaApp);
  return userClient.query(schemaApp.todos.where({ project: folderId }));
}
