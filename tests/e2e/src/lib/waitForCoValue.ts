import {
  Account,
  CoValue,
  CoValueClass,
  ID,
  RefsToResolve,
  subscribeToCoValue,
} from "jazz-tools";

export function waitForCoValue<
  T extends CoValue,
  const R extends RefsToResolve<T>,
>(
  coMap: CoValueClass<T>,
  valueId: ID<T>,
  predicate: (value: T) => boolean,
  options: { loadAs: Account; resolve?: R },
) {
  return new Promise<T>((resolve) => {
    function subscribe() {
      subscribeToCoValue(
        coMap,
        valueId,
        options,
        (value, unsubscribe) => {
          if (predicate(value)) {
            resolve(value);
            unsubscribe();
          }
        },
        () => {
          setTimeout(subscribe, 100);
        },
      );
    }

    subscribe();
  });
}
