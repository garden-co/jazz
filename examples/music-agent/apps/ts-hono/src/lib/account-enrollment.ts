/** First open: the server seeds this user's workspace (idempotent). */
export async function bootstrapWorkspace(): Promise<Response> {
  return await fetch("/api/bootstrap", { method: "POST", credentials: "same-origin" });
}
