/** Browser-side calls to the agent routes. Replies themselves arrive through Jazz, not these responses. */
async function post(path: string, body?: unknown): Promise<string> {
  const response = await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const result = (await response.json()) as { turnId?: string; error?: string };
  if (!response.ok || !result.turnId)
    throw new Error(result.error ?? `Request failed (${response.status})`);
  return result.turnId;
}

export const requestReply = (userTurnId: string) => post("/api/turns", { userTurnId });
export const regenerateReply = (turnId: string) => post(`/api/turns/${turnId}/regenerate`);
export const resumeReply = (turnId: string) => post(`/api/turns/${turnId}/resume`);
