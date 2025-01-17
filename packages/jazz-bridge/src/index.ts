import {
  Account,
  CoMap,
  CoValue,
  CoValueBase,
  CoValueClass,
  CoValueClassWithLoad,
  Group,
  ID,
  co,
} from "jazz-tools";

class ExtState extends CoMap {
  extID = co.optional.string;
  state = co.literal(
    "creating",
    "fetching",
    "filling_refs",
    "pushing",
    "synced",
    "errored",
  );
  lastError = co.optional.string;
}

class CoValuesByExtID extends CoMap.Record(co.json<ID<CoValue>>()) {}
class ExtStatesByCoID extends CoMap.Record(co.optional.ref(ExtState)) {}

export function bridgeExternalSystem<ExtID extends string>(options: {
  systemName: string;
  mappings: Mapping<any, ExtID, any>[];
  worker: Account;
}) {
  async function getCoValuesByExtID() {
    const coValuesByExtID = await CoValuesByExtID.getUnique(
      {
        systemName: options.systemName,
      },
      options.worker.id,
      options.worker,
    );
    if (!coValuesByExtID) {
      throw new Error("Couldn't load coValuesByExtID");
    }
    return coValuesByExtID;
  }

  async function getExtStatesByCoID() {
    const extStatesByCoID = await (
      await ExtStatesByCoID.getUnique(
        {
          systemName: options.systemName,
        },
        options.worker.id,
        options.worker,
      )
    ).ensureLoaded([{}]);
    if (!extStatesByCoID) {
      throw new Error("Couldn't load extStatesByCoID");
    }
    return extStatesByCoID;
  }

  function mappingFor<T extends CoValue>(
    coValueClass: CoValueClass<T>,
  ): Mapping<T, ExtID, any> {
    const mapping = options.mappings.find((m) => m.Schema === coValueClass);
    if (!mapping) {
      throw new Error(`No mapping found for ${coValueClass.name}`);
    }
    return mapping as Mapping<T, ExtID, any>;
  }

  async function process(coValue: CoValue, token: string) {
    const coValuesByExtID = await getCoValuesByExtID();
    const extStatesByCoID = await getExtStatesByCoID();

    const mapping = mappingFor(coValue.constructor as CoValueClass<CoValue>);

    let extState = extStatesByCoID[coValue.id];
    if (!extState) {
      if (mapping.sync.type === "two-way") {
        extState = ExtState.create(
          {
            state: "creating",
          },
          { owner: options.worker },
        );
        extStatesByCoID[coValue.id] = extState;

        try {
          // TODO: do relations first
          const extID = await mapping?.sync.createExt(coValue, token);
          extState.extID = extID;
          extState.state = "synced";
        } catch (e: unknown) {
          extState.state = "errored";
          extState.lastError = e instanceof Error ? e.message : String(e);
        }
      } else {
        console.debug(
          `Not creating external value for ${coValue.id} because ${coValue.constructor.name} only sync one-way`,
        );
      }
    }
  }

  function getOrCreateExtDepFor(token: string, additionalMember: Account) {
    return async function getOrCreateExtDep<C extends CoValue>(
      coValueClass: CoValueClass<C>,
      id: ExtID,
      prefetchedExt?: any,
    ): Promise<C> {
      const coValuesByExtID = await getCoValuesByExtID();
      const extStatesByCoID = await getExtStatesByCoID();

      const mapping = mappingFor(coValueClass);

      if (mapping.sync.type !== "to-jazz" && mapping.sync.type !== "two-way") {
        throw new Error("Not implemented");
      }

      const existingCoValueID = coValuesByExtID[id as string] as ID<C>;

      let depCoValue: C;

      if (existingCoValueID) {
        console.log(`found ${coValueClass.name} ${id} ${existingCoValueID}`);

        depCoValue = await (coValueClass as CoValueClassWithLoad<C>).load(
          existingCoValueID,
          options.worker,
          [],
        );
      } else {
        let extState: any;
        if (prefetchedExt) {
          console.log(
            `using prefetched ${coValueClass.name} ${id} ${JSON.stringify(prefetchedExt)}`,
          );
          extState = prefetchedExt;
        } else {
          console.log(`fetching ${coValueClass.name} ${id}`);
          extState = await mapping.sync.fetch(id, token);
          console.log(
            `fetched ${coValueClass.name} ${id} ${JSON.stringify(extState)}`,
          );
        }
        const group = Group.create({ owner: options.worker });
        group.addMember(additionalMember, "reader");

        const coValue = await mapping.sync.createFromExt(id, extState, group);

        coValuesByExtID[id as string] = coValue.id;
        console.log(
          "set coValuesByExtID[id as string]",
          id,
          coValuesByExtID[id as string],
          coValue.id,
        );

        extStatesByCoID[coValue.id] = ExtState.create(
          {
            state: "filling_refs",
          },
          { owner: options.worker },
        );

        console.log(`filling references for ${coValueClass.name} ${id}`);
        await mapping.sync.fillReferencesFromExt?.(
          id,
          extState,
          coValue,
          token,
          getOrCreateExtDepFor(token, additionalMember),
        );

        extStatesByCoID[coValue.id] = ExtState.create(
          {
            state: "synced",
          },
          { owner: options.worker },
        );

        console.log(`created ${coValueClass.name} ${id} ${coValue.id}`);

        depCoValue = await coValue;
      }

      // if (mapping.sync.type === "two-way") {
      //   const sync = mapping.sync;
      //   (depCoValue as unknown as CoMap).subscribe({procedures: []}, async (coValue) => {
      //     console.log(`${coValueClass.name} ${depCoValue.id} changed, need to update in external system`);
      //     async function createOrPushEditsToExtDep(coValue: CoValue) {
      //       const coValueClass = coValue.constructor as CoValueClass<CoValue>;
      //       const mapping = mappingFor(coValueClass);
      //       if (mapping.sync.type !== "two-way") {
      //         throw new Error("Not implemented");
      //       }
      //       await mapping.sync.createExt(coValue, token);
      //     }

      //     // await sync.pushEditsToExt(id, coValue as C, createOrPushEditsToExtDep);
      //   });
      // }

      return depCoValue;
    };
  }

  return {
    invalidateExt(id: ExtID[] | "all") {},
    async createOrUpdateRootInJazz<C extends CoValue>(
      id: ExtID,
      coValueClass: CoValueClassWithLoad<C>,
      additionalMember: Account,
      currentToken: string,
    ): Promise<C> {
      const coValuesByExtID = await getCoValuesByExtID();
      const extStatesByCoID = await getExtStatesByCoID();
      const existingCoValueID = coValuesByExtID[id as string] as ID<C>;

      if (existingCoValueID) {
        const existingCoValue = (await coValueClass.load(
          existingCoValueID,
          options.worker,
          [],
        )) as C;

        if (!existingCoValue) {
          throw new Error(
            `Couldn't load existing CoValue ${existingCoValueID}`,
          );
        }

        await process(existingCoValue, currentToken);

        return existingCoValue;
      } else {
        const mapping = mappingFor(coValueClass);
        if (mapping.sync.type !== "to-jazz") {
          throw new Error(
            "Creating account roots for non 'to-jazz' mapping not implemented",
          );
        }
        const ext = await mapping.sync.fetch(id, currentToken);
        const group = Group.create({ owner: options.worker });
        group.addMember(additionalMember, "reader");

        const coValue = await mapping.sync.createFromExt(id, ext, group);

        coValuesByExtID[id as string] = coValue.id;
        extStatesByCoID[coValue.id] = ExtState.create(
          {
            state: "synced",
          },
          { owner: options.worker },
        );

        await mapping.sync.fillReferencesFromExt?.(
          id,
          ext,
          coValue,
          currentToken,
          getOrCreateExtDepFor(currentToken, additionalMember),
        );

        console.log("created root", coValueClass.name, id, coValue.id);

        return coValue;
      }
    },
  };
}

type Mapping<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
> = {
  Schema: CoValueClass<C>;
  sync:
    | ToJazzSync<C, ExtID, ExtValue>
    | ToExtSync<C, ExtID, ExtValue>
    | TwoWaySync<C, ExtID, ExtValue>;
};

type ToJazzParams<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
> = {
  fetch: (id: ExtID, token: string) => Promise<ExtValue>;

  createFromExt: (id: ExtID, ext: ExtValue, owner: Group) => Promise<C>; // the return type might only be partial, this is slightly wrong

  fillReferencesFromExt?: (
    id: ExtID,
    ext: ExtValue,
    partial: C,
    token: string,
    getOrCreateExtDep: <DC extends CoValue>(
      coValueClass: CoValueClass<DC>,
      id: ExtID,
      prefetchedExt?: any,
    ) => Promise<DC>,
  ) => Promise<void>;

  updateFromExt: (id: ExtID, ext: ExtValue, coValue: C) => Promise<void>;

  updateReferencesFromExt?: (
    id: ExtID,
    ext: ExtValue,
    coValue: C,
    token: string,
    updateOrCreateExtDep: <C extends CoValue>(
      coValueClass: CoValueClass<C>,
      id: ExtID,
      prefetchedExt?: any,
    ) => Promise<C>,
  ) => Promise<void>;
};

type ToExtParams<C extends CoValue, ExtID extends string> = {
  createExt: (coValue: C, token: string) => Promise<ExtID>;

  pushEditsToExt: (
    id: ExtID,
    coValue: C,
    createOrPushEditsToExtDep: <D extends CoValue>(dep: D) => Promise<void>,
  ) => Promise<void>;
};

type ToJazzSync<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
> = {
  type: "to-jazz";
} & ToJazzParams<C, ExtID, ExtValue>;

type ToExtSync<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
> = {
  type: "to-ext";
} & ToExtParams<C, ExtID>;

type TwoWaySync<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
> = {
  type: "two-way";
} & ToJazzParams<C, ExtID, ExtValue> &
  ToExtParams<C, ExtID>;

export function mapping<
  C extends CoValue,
  ExtID extends string,
  ExtValue extends object,
>(
  Schema: CoValueClass<C>,
  sync:
    | ToJazzSync<C, ExtID, ExtValue>
    | ToExtSync<C, ExtID, ExtValue>
    | TwoWaySync<C, ExtID, ExtValue>,
): Mapping<C, ExtID, ExtValue> {
  return { Schema, sync };
}
