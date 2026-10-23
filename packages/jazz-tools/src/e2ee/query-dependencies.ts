import type { Db, QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { WasmSchema } from "../drivers/types.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import { encryptedSchemas } from "./encrypted-schema.js";
import { e2eeAccountForDb, withSpaceKeys } from "./lifecycle.js";

export type QuerySpace = { scope: string; identifier: string };
// Private result metadata must survive public projections and mutable returned values.
export const decryptedQuerySpaces = new WeakMap<object, QuerySpace[]>();

/** Observe spaces needed to decrypt and verify the current candidate result. */
export function queryKeyDependencies(
  db: Db,
  schema: WasmSchema,
  onChange: () => void,
  onError: (error: Error) => void,
  options: QueryOptions,
) {
  const spaces = new Map<string, QuerySpace & { stop: () => void }>();
  const common: Array<() => void> = [];
  let started = false;
  let stopped = false;
  const watch = (query: QueryBuilder<{ id: string }>) =>
    db.subscribe(query, { onUpdate: onChange, onError }, options);
  return {
    async update(required: QuerySpace[]) {
      if (stopped) return;
      if (!required.length) {
        started = false;
        for (const stop of common) stop();
        common.length = 0;
      }
      if (!started && required.length) {
        started = true;
        const queries: QueryBuilder<{ id: string }>[] = [
          new TypedTableQueryBuilder("__e2ee_account_identities", schema)
            .where({ id: e2eeAccountForDb(db) })
            .include({
              __e2ee_public_account_successorsViaAccount: true,
              __e2ee_public_device_approvalsViaAccount: true,
            }),
        ];
        // ponytail: observe the readable group graph; narrow to the accepted
        // dependency closure if group-graph scale makes this too expensive.
        if (encryptedSchemas.get(schema)?.logical.__e2ee_groups)
          queries.push(
            new TypedTableQueryBuilder("__e2ee_groups", schema).include({
              __e2ee_group_membershipViaGroup: true,
              __e2ee_group_successorsViaGroup: true,
              __e2ee_group_deliveriesViaGroup: true,
              __e2ee_group_repairsViaGroup: true,
            }),
          );
        for (const query of queries) {
          if (stopped) return;
          const stop = watch(query);
          if (stopped) stop();
          else common.push(stop);
        }
      }
      const wanted = new Map(
        required.map((space) => [JSON.stringify([space.scope, space.identifier]), space]),
      );
      for (const [key, space] of spaces) {
        if (!wanted.has(key)) {
          spaces.delete(key);
          space.stop();
        }
      }
      for (const [key, space] of wanted) {
        if (stopped) return;
        if (spaces.has(key)) continue;
        const scopeId = await db.tableIdentity(new TypedTableQueryBuilder(space.scope, schema));
        if (stopped) return;
        const entry = { ...space, stop: () => {} };
        spaces.set(key, entry);
        const stop = watch(
          new TypedTableQueryBuilder("__e2ee_spaces", schema)
            .where({ scopeId, identifier: space.identifier })
            .include({
              __e2ee_space_successorsViaSpace: true,
              __e2ee_space_grantsViaSpace: true,
              __e2ee_space_deliveriesViaSpace: true,
            }),
        );
        if (stopped || spaces.get(key) !== entry) stop();
        else entry.stop = stop;
      }
    },
    async validate(equalitySpace?: QuerySpace) {
      for (const space of spaces.values()) {
        if (stopped) return;
        // Equality's epoch-set check validates this same root immediately afterwards.
        if (space.scope === equalitySpace?.scope && space.identifier === equalitySpace.identifier)
          continue;
        await withSpaceKeys(
          db,
          new TypedTableQueryBuilder(space.scope, schema),
          space.identifier,
          async () => {},
          true,
        );
      }
    },
    stop() {
      stopped = true;
      for (const stop of common) stop();
      for (const space of spaces.values()) space.stop();
      common.length = 0;
      spaces.clear();
    },
  };
}
