/** Serialises preparation and staging, without waiting for write durability. */
export class WritePreparation {
  pending?: Promise<void>;
  private error?: unknown;
  failed = false;
  get failure(): unknown {
    return this.error;
  }
  set failure(error: unknown) {
    this.failed = true;
    this.error = error;
  }
  cancelled = false;

  constructor(private readonly failClosed = false) {}

  run<T>(operation: () => T | Promise<T>): T | Promise<T> {
    const start = () => {
      if (this.failed) throw this.failure;
      return operation();
    };
    const result = this.pending ? this.pending.then(start) : start();
    if (result instanceof Promise) {
      const tail = result.then(
        () => {},
        (error) => {
          if (this.failClosed) this.failure = error;
        },
      );
      this.pending = tail;
      tail.then(() => {
        if (this.pending === tail) this.pending = undefined;
      });
    }
    return result;
  }

  async drain(): Promise<void> {
    await this.pending;
    if (this.failed) throw this.failure;
  }
}

const owners = new WeakMap<object, WritePreparation>();
export function getOwnerPreparation(
  client: object & { preparationOwner?: () => object },
): WritePreparation | undefined {
  return owners.get(client.preparationOwner?.() ?? client);
}

export function ownerPreparation(
  client: object & { preparationOwner?: () => object },
): WritePreparation {
  const owner = client.preparationOwner?.() ?? client;
  let preparation = owners.get(owner);
  if (!preparation) owners.set(owner, (preparation = new WritePreparation()));
  return preparation;
}

/** Ordinary synchronous operations need no preparation object. */
export function runAfterPreparation<T>(
  client: object & { preparationOwner?: () => object },
  operation: () => T | Promise<T>,
): T | Promise<T> {
  const preparation = getOwnerPreparation(client);
  if (preparation?.pending) return preparation.run(operation);
  const result = operation();
  return result instanceof Promise ? ownerPreparation(client).run(() => result) : result;
}
