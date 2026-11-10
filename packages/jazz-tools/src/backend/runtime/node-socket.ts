/** Node's WebSocket constructor supports authenticated upgrade headers. */
export function openWebSocket(url: string, headers: string[][], _signal: AbortSignal): WebSocket {
  const Socket = globalThis.WebSocket as unknown as new (
    url: string,
    options: { headers: Record<string, string> },
  ) => WebSocket;
  if (!Socket) throw new Error("remote() needs a global WebSocket (Node 22 or newer)");
  return new Socket(url, { headers: Object.fromEntries(headers) });
}
