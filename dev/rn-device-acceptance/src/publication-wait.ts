const PUBLICATION_TURNS = 8;

function yieldEventLoopTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Give a native CallInvoker wake a bounded number of event-loop turns. */
export async function waitForPublication(
  observed: () => boolean,
  yieldTurn: () => Promise<void> = yieldEventLoopTurn,
): Promise<boolean> {
  for (let attempt = 0; attempt < PUBLICATION_TURNS && !observed(); attempt += 1) {
    await yieldTurn();
  }
  return observed();
}
