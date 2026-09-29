/** Next runs this once per server start. */
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  // Replies left streaming by a previous server process are marked
  // interrupted, so the conversation offers to resume or retry them.
  const { startSweeper } = await import("./src/agent/runner");
  startSweeper();
}
