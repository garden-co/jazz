/**
 * An in-memory account store for a session in a test (`createJazzClient({
 * store })` / `<JazzProvider config={{ store }}>`). Each store keeps its own
 * device account, so mounts with separate stores are separate users; mounts
 * sharing one store are the same user on two replicas.
 */
export function memoryAccountStore(): {
  read(): Promise<string | null>;
  update(transform: (current: string | null) => string): Promise<void>;
} {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
}
