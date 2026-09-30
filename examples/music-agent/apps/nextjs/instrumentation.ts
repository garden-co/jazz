/** Next runs this once per server start. */
export async function register() {
  // Guarded this way (not an early return) so the edge build drops the import.
  if (process.env.NEXT_RUNTIME === "nodejs") {
    // Fail at boot, not on the first reply, when the provider is misconfigured.
    const { agentProvider } = await import("./src/agent/config");
    agentProvider();
    // Replies left streaming by a previous server process are marked
    // interrupted, so the conversation offers to resume or retry them.
    const { startSweeper } = await import("./src/agent/runner");
    startSweeper();
  }
}
