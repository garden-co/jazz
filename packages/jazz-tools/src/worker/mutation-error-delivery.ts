import type { MutationErrorEvent } from "../runtime/client.js";

/**
 * Deliver a rejection only to runtimes that are attached at the instant the
 * worker observes it. This is deliberately stateless: reconciliation is
 * durable, but a foreground application's notification is not.
 */
export function deliverMutationErrorToAttachedPeers<TPeer>(
  peers: Iterable<TPeer>,
  event: MutationErrorEvent,
  deliver: (peer: TPeer, event: MutationErrorEvent) => void,
): void {
  for (const peer of peers) deliver(peer, event);
}
