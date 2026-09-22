// Host persistence is optional for injected/in-memory account managers. Keep the
// asynchronous durability boundary internal so low-level local-first creation
// remains synchronous while high-level session commands can await it.
const barriers = new WeakMap<object, (retry: boolean) => Promise<void>>();
export function setAccountSelectionBarrier(
  manager: object,
  barrier: (retry: boolean) => Promise<void>,
): void {
  barriers.set(manager, barrier);
}
export function settleAccountSelection(manager: object, retry = false): Promise<void> {
  return barriers.get(manager)?.(retry) ?? Promise.resolve();
}
