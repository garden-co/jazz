import type { Request, Response } from "express";
import type { JazzClient } from "jazz-tools/backend";
import { app as schemaApp } from "../schema.js";

declare const client: JazzClient;

function sendQueryError(res: Response): void {
  res.status(500).json({ error: "Failed to query todos" });
}

// #region backend-request-handler-ts
export async function listTodosForRequester(req: Request, res: Response): Promise<void> {
  try {
    const requester = await client.forRequest(req);
    const rows = await requester.all(schemaApp.todos.where({ done: true }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(req: Request, res: Response): Promise<void> {
  try {
    const requester = await client.forRequest(req);
    const rows = await requester.all(schemaApp.todos.where({ done: false }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion permissions-simple-ts

// #region permissions-inherits-ts
export async function listTodosWithInheritedPolicy(
  req: Request<{ projectId: string }>,
  res: Response,
): Promise<void> {
  try {
    const requester = await client.forRequest(req);
    const rows = await requester.all(schemaApp.todos.where({ projectId: req.params.projectId }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion permissions-inherits-ts

// #region backend-attribution-ts
export async function createAttributedHandles(req: Request) {
  return {
    backendDb: client.db,
    requesterDb: await client.forRequest(req),
    attributedRequestDb: await client.withAttributionForRequest(req),
  };
}
// #endregion backend-attribution-ts
