import type { Request } from "express";
import { JazzClient, type Session } from "jazz-ts";

type RequesterIdentity = {
  userId: string;
  claims: Record<string, unknown>;
};

function verifyJwtAndExtractIdentity(_jwt: string): RequesterIdentity {
  // Replace with your auth provider's JWT verification logic.
  return { userId: "replace-with-verified-sub", claims: {} };
}

function buildQuery(table: string): string {
  return JSON.stringify({
    table,
    branches: [],
    disjuncts: [{ conditions: [] }],
    order_by: [],
    offset: 0,
    include_deleted: false,
    array_subqueries: [],
    joins: [],
  });
}

function buildFolderScopedQuery(folderId: string): string {
  return JSON.stringify({
    table: "todos",
    branches: [],
    disjuncts: [
      {
        conditions: [
          {
            column: "folder_id",
            op: "Eq",
            value: { type: "Uuid", value: folderId },
          },
        ],
      },
    ],
    order_by: [],
    offset: 0,
    include_deleted: false,
    array_subqueries: [],
    joins: [],
  });
}

// #region backend-request-session-ts
export function sessionFromRequest(req: Request): Session {
  const auth = req.header("authorization");
  if (!auth?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const jwt = auth.slice("Bearer ".length);
  const identity = verifyJwtAndExtractIdentity(jwt);
  return { user_id: identity.userId, claims: identity.claims };
}
// #endregion backend-request-session-ts

// #region backend-request-handler-ts
export async function listTodosForRequester(req: Request, client: JazzClient) {
  const userClient = client.forSession(sessionFromRequest(req));
  const rows = await userClient.query(buildQuery("todos"));
  return rows;
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(req: Request, client: JazzClient) {
  const userClient = client.forSession(sessionFromRequest(req));
  return userClient.query(buildQuery("todos"));
}
// #endregion permissions-simple-ts

// #region permissions-inherits-ts
export async function listTodosWithInheritedPolicy(
  req: Request,
  client: JazzClient,
  folderId: string,
) {
  const userClient = client.forSession(sessionFromRequest(req));
  return userClient.query(buildFolderScopedQuery(folderId));
}
// #endregion permissions-inherits-ts
