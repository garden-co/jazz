import type { Request, Response } from "express";
import type { JazzClient } from "jazz-tools/backend";
import { app } from "../schema/app.js";

declare const client: JazzClient;

function sendQueryError(res: Response): void {
  res.status(500).json({ error: "Failed to query todos" });
}

// #region backend-request-handler-ts
export async function listTodosForRequester(req: Request, res: Response): Promise<void> {
  try {
    const rows = await client.forRequest(req).query(app.todos.where({ done: true }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(req: Request, res: Response): Promise<void> {
  try {
    const rows = await client.forRequest(req).query(app.todos.where({ done: false }));
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
    const rows = await client
      .forRequest(req)
      .query(app.todos.where({ project: req.params.projectId }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion permissions-inherits-ts
