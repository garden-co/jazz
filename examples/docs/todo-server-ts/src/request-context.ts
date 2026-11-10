import type { Jazz } from "jazz-tools/backend";
import { app as schemaApp } from "../schema.js";

declare const jazz: Jazz;

function queryError(): Response {
  return Response.json({ error: "Failed to query todos" }, { status: 500 });
}

// #region backend-request-handler-ts
export async function listTodosForRequester(request: Request): Promise<Response> {
  try {
    const requester = await jazz.forReqUser(request);
    const rows = await requester.all(schemaApp.todos.where({ done: true }));
    return Response.json(rows);
  } catch {
    return queryError();
  }
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(request: Request): Promise<Response> {
  try {
    const requester = await jazz.forReqUser(request);
    const rows = await requester.all(schemaApp.todos.where({ done: false }));
    return Response.json(rows);
  } catch {
    return queryError();
  }
}
// #endregion permissions-simple-ts

// #region permissions-inherits-ts
export async function listTodosWithInheritedPolicy(
  request: Request,
  projectId: string,
): Promise<Response> {
  try {
    const requester = await jazz.forReqUser(request);
    const rows = await requester.all(schemaApp.todos.where({ projectId }));
    return Response.json(rows);
  } catch {
    return queryError();
  }
}
// #endregion permissions-inherits-ts

// #region backend-attribution-ts
export async function createAttributedHandles(request: Request) {
  return {
    backendDb: jazz.db,
    requesterDb: await jazz.forReqUser(request),
    attributedRequestDb: await jazz.withAttribution(request),
  };
}
// #endregion backend-attribution-ts
