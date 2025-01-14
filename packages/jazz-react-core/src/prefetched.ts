import { CoValue } from "jazz-tools";

export type Prefetched<V extends CoValue> = {
  [K in keyof V]: V[K] extends CoValue
    ? Prefetched<V[K]>
    : V[K] extends Function
      ? never
      : V[K];
};

export function serializePrefetched<V extends CoValue>(
  value: V,
): Prefetched<V> {
  return value.toJSON() as Prefetched<V>;
}
