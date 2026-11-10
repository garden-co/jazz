export async function openWebSocket(
  url: string,
  headers: string[][],
  signal: AbortSignal,
): Promise<WebSocket> {
  const response = await fetch(url.replace(/^ws/, "http"), {
    headers: [
      ...headers.map(([name, value]) => [name!, value!] as [string, string]),
      ["Upgrade", "websocket"],
    ],
    signal,
  });
  const socket = (response as Response & { webSocket?: WebSocket & { accept(): void } }).webSocket;
  if (response.status !== 101 || !socket) {
    await response.body?.cancel();
    throw new Error(`Jazz core WebSocket upgrade failed (${response.status})`);
  }
  socket.binaryType = "arraybuffer";
  socket.accept();
  return socket;
}
