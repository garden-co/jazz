import {
  Account,
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  ID,
  RefsToResolve,
  RefsToResolveStrict,
  subscribeToCoValue,
} from "jazz-tools";

export function waitForCoValue<
  T extends CoValue,
  const R extends RefsToResolve<T>,
>(
  coMap: CoValueClass<T>,
  valueId: ID<T>,
  predicate: (value: T) => boolean,
  options: {
    loadAs: Account | AnonymousJazzAgent;
    resolve?: RefsToResolveStrict<T, R>;
  },
) {
  return new Promise<T>((resolve) => {
    function subscribe() {
      subscribeToCoValue(
        coMap,
        valueId,
        {
          loadAs: options.loadAs,
          resolve: options.resolve,
        },
        (value, unsubscribe) => {
          if (value.$isLoaded && predicate(value)) {
            resolve(value);
            unsubscribe();
          }
        },
      );
    }

    subscribe();
  });
}
