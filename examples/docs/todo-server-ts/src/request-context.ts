import type { Request, Response } from "express";
import type { JazzContext } from "jazz-tools/backend";
import { app as schemaApp } from "../schema/app.js";

declare const context: JazzContext;

function sendQueryError(res: Response): void {
  res.status(500).json({ error: "Failed to query todos" });
}

// #region backend-request-handler-ts
export async function listTodosForRequester(req: Request, res: Response): Promise<void> {
  try {
    const rows = await context
      .forRequest(req, schemaApp)
      .query(schemaApp.todos.where({ done: true }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion backend-request-handler-ts

// #region permissions-simple-ts
export async function listTodosWithSimplePolicy(req: Request, res: Response): Promise<void> {
  try {
    const rows = await context
      .forRequest(req, schemaApp)
      .query(schemaApp.todos.where({ done: false }));
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
    const rows = await context
      .forRequest(req, schemaApp)
      .query(schemaApp.todos.where({ project: req.params.projectId }));
    res.json(rows);
  } catch {
    sendQueryError(res);
  }
}
// #endregion permissions-inherits-ts
