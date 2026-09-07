export interface SessionAtom<T extends { isPending: boolean }> {
  get(): T;
  subscribe(listener: (value: T) => void): () => void;
}

/** Wait for Better Auth's initial session read, including synchronous listeners. */
export async function waitForInitialSession<T extends { isPending: boolean }>(
  sessionAtom: SessionAtom<T>,
): Promise<void> {
  if (!sessionAtom.get().isPending) return;

  await new Promise<void>((resolve) => {
    let unsubscribe: (() => void) | undefined;
    let ready = false;
    const onSession = (next: T) => {
      if (next.isPending) return;
      ready = true;
      unsubscribe?.();
      resolve();
    };
    unsubscribe = sessionAtom.subscribe(onSession);
    // Better Auth atoms may notify synchronously during subscribe, before the
    // unsubscribe function is assigned.
    if (ready) unsubscribe();
  });
}
