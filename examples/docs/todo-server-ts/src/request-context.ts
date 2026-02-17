import type { Request } from "express";
import { JazzClient } from "jazz-tools/backend";

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
export function scopedClientFromRequest(req: Request, client: JazzClient) {
  return client.forRequest(req);
}
// #endregion backend-request-session-ts

// #region backend-request-handler-ts
export async function listTodosForRequester(req: Request, client: JazzClient) {
  const userClient = scopedClientFromRequest(req, client);
  const rows = await userClient.query(buildQuery("todos"));
  return rows;
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(req: Request, client: JazzClient) {
  const userClient = scopedClientFromRequest(req, client);
  return userClient.query(buildQuery("todos"));
}
// #endregion permissions-simple-ts

// #region permissions-inherits-ts
export async function listTodosWithInheritedPolicy(
  req: Request,
  client: JazzClient,
  folderId: string,
) {
  const userClient = scopedClientFromRequest(req, client);
  return userClient.query(buildFolderScopedQuery(folderId));
}
// #endregion permissions-inherits-ts
