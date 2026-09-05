const PUBLICATION_DEADLINE_MS = 30_000;

function yieldEventLoopTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * Keep servicing native CallInvoker wakes until a concrete subscription
 * marker arrives or the device receipt's bounded propagation allowance ends.
 * This waits for the marker itself, never a nominal number of relay turns or
 * an empty LocalFirst opening.
 */
export async function waitForPublication(
  observed: () => boolean,
  yieldTurn: () => Promise<void> = yieldEventLoopTurn,
  now: () => number = Date.now,
): Promise<boolean> {
  const deadline = now() + PUBLICATION_DEADLINE_MS;
  while (!observed() && now() < deadline) {
    await yieldTurn();
  }
  return observed();
}
