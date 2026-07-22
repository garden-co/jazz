import { AuthSecretStore, DbConfig, QueryBuilder, QueryOptions } from "jazz-tools";
import {
  abortVar,
  action,
  atom,
  type Atom,
  computed,
  effect,
  isShallowEqual,
  named,
  ReatomError,
  retryComputed,
  withConnectHook,
  withSuspense,
  withSuspenseInit,
  withSuspenseRetry,
  wrap,
} from "@reatom/core";
import { createJazzClient } from "jazz-tools/react";

type CachedQueryOptions<Args extends readonly unknown[]> = {
  name?: string;
  equals?: (a: Args, b: Args) => boolean;
};

export const createJazz = (config: DbConfig | (() => DbConfig), name: string = named("jazz")) => {
  const client = computed(
    () => createJazzClient(typeof config === "function" ? config() : config),
    `${name}.client`,
  ).extend(withSuspense({ preserve: true }));

  const suspendedClient = client.suspended;

  const reatomQueryAll = <T extends { id: string }>(
    queryBuilder: () => QueryBuilder<T> | { query: QueryBuilder<T>; options: QueryOptions },
    name: string,
  ) => {
    const getCacheEntry = () => {
      const { manager } = suspendedClient();
      const factoryResult = queryBuilder();

      const { query, options } =
        "query" in factoryResult ? factoryResult : { query: factoryResult };
      const key = manager.makeQueryKey(query, options);

      return manager.getCacheEntry<T>(key);
    };

    const getCacheEntryAsync = action(async () => getCacheEntry()).extend(withSuspenseRetry());

    const result = atom(async () => {
      const entry = await wrap(getCacheEntryAsync());
      if (entry.state.status === "fulfilled") return entry.state.data;
      else if (entry.state.status === "rejected") {
        throw new ReatomError(`Jazz query failed (${name}): ${entry.state.error}`);
      }

      return wrap(entry.promise);
    }, name).extend(
      withSuspenseInit(),
      withConnectHook((target) => {
        effect(() => {
          const entry = getCacheEntry();

          const unsub = entry.subscribe({
            onfulfilled: wrap(target.set),
            onDelta: wrap((data) => target.set(data.all)),
          });

          abortVar.subscribe(unsub);
        }, `${name}.subcription`);
      }),
    );

    return result;
  };

  const reatomCachedQuery = <Args extends readonly unknown[], T extends { id: string }>(
    queryBuilder: (
      ...args: Args
    ) => QueryBuilder<T> | { query: QueryBuilder<T>; options: QueryOptions },
    nameOrOptions: string | CachedQueryOptions<Args> = {},
  ): ((...args: Args) => Atom<T[]>) => {
    const options = typeof nameOrOptions === "string" ? { name: nameOrOptions } : nameOrOptions;
    const baseName = options.name ?? named("cachedQuery");
    const equals = options.equals ?? isShallowEqual;
    const cache: Array<{ args: Args; atom: Atom<T[]> }> = [];

    return (...args: Args) => {
      let entry = cache.find((e) => equals(e.args, args));
      if (!entry) {
        const atom = reatomQueryAll(() => queryBuilder(...args), `${baseName}#[${args}]`);
        entry = { args, atom };
        cache.push(entry);
      }
      return entry.atom;
    };
  };

  return suspendedClient.extend(() => ({
    reatomQueryAll,
    reatomCachedQuery,
  }));
};

export const reatomJazzLocalFirstAuth = (
  storage: AuthSecretStore,
  name: string = named("jazzLocalFirstAuth"),
) => {
  const secretAtom = computed(() => storage.getOrCreateSecret(), `${name}.secret`).extend(
    withSuspense(),
  );

  const login = action(async (secret: string) => {
    await wrap(storage.saveSecret(secret));
    retryComputed(secretAtom);
  }, `${name}.login`);

  const signOut = action(async () => {
    await wrap(storage.clearSecret());
    retryComputed(secretAtom);
  }, `${name}.signOut`);

  return secretAtom.suspended.extend(() => ({
    login,
    signOut,
  }));
};
