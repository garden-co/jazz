/** Errors of the private store contract (see `private-store.ts`). */

/** The save found another revision than `expected`: read again and merge. */
export class PrivateStoreConflictError extends Error {
  constructor(message = "the private store changed since it was read") {
    super(message);
    this.name = "PrivateStoreConflictError";
  }
}

/** The scope was deleted (and perhaps made again) since it was opened: nothing is written. */
export class PrivateStoreWipedError extends Error {
  constructor(message = "the private store was wiped since it was opened") {
    super(message);
    this.name = "PrivateStoreWipedError";
  }
}
