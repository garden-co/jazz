// Rust `block_on` drives ready-immediate futures with a no-op waker. JS cannot
// block on a promise; callers of the local lane just await instead.
export const blockOn = <T>(future: Promise<T>): Promise<T> => future;
