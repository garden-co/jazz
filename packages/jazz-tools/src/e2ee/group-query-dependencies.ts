import type { QueryBuilder } from "../runtime/db.js";
import type { WasmSchema } from "../drivers/types.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import type { GroupMembership } from "./groups.js";

export type DependencyWatch = (
  query: QueryBuilder<{ id: string }>,
  onRows?: (rows: { id: string }[]) => void,
) => () => void;

/** Watch only the raw historical component that can affect the required groups.
 * Incoming edges matter too: they can turn an otherwise valid edge into a cycle.
 * Routing candidates never authorise membership; accepted replay still does that.
 */
export function groupQueryDependencies(schema: WasmSchema, watch: DependencyWatch) {
  const roots = new Map<string, string[]>();
  const groups = new Map<
    string,
    { outgoing: string[]; incoming: string[]; stops: (() => void)[] }
  >();
  let stopped = false;
  let reconciling = false;
  let dirty = false;
  const reconcile = () => {
    if (stopped) return;
    dirty = true;
    if (reconciling) return;
    reconciling = true;
    try {
      while (dirty && !stopped) {
        dirty = false;
        const wanted = new Set<string>();
        const pending = [...roots.values()].flat();
        for (let i = 0; i < pending.length; i++) {
          const id = pending[i]!;
          if (wanted.has(id)) continue;
          wanted.add(id);
          const group = groups.get(id);
          if (group) pending.push(...group.outgoing, ...group.incoming);
        }
        for (const [id, group] of groups) {
          if (wanted.has(id)) continue;
          groups.delete(id);
          for (const stop of group.stops) stop();
        }
        for (const id of wanted) {
          if (stopped) break;
          if (groups.has(id)) continue;
          const group = {
            outgoing: [] as string[],
            incoming: [] as string[],
            stops: [] as (() => void)[],
          };
          groups.set(id, group);
          const add = (
            query: QueryBuilder<{ id: string }>,
            onRows?: (rows: { id: string }[]) => void,
          ) => {
            if (stopped || groups.get(id) !== group) return;
            const stop = watch(query, onRows);
            if (stopped || groups.get(id) !== group) stop();
            else group.stops.push(stop);
          };
          const members = (direction: "outgoing" | "incoming") => (rows: { id: string }[]) => {
            if (stopped || groups.get(id) !== group) return;
            // These rows come only from the managed membership table below.
            group[direction] = (rows as GroupMembership[])
              .filter((row) => row.memberKind === "group")
              .map((row) => (direction === "outgoing" ? row.memberId : row.groupId));
            reconcile();
          };
          add(
            new TypedTableQueryBuilder("__e2ee_groups", schema).where({ id }).include({
              __e2ee_group_successorsViaGroup: true,
              __e2ee_group_deliveriesViaGroup: true,
              __e2ee_group_repairsViaGroup: true,
            }),
          );
          add(
            new TypedTableQueryBuilder("__e2ee_group_membership", schema).where({ groupId: id }),
            members("outgoing"),
          );
          add(
            new TypedTableQueryBuilder("__e2ee_group_membership", schema).where({
              memberKind: "group",
              memberId: id,
            }),
            members("incoming"),
          );
        }
      }
    } finally {
      reconciling = false;
    }
  };
  return {
    update(space: string, ids: string[]) {
      roots.set(space, ids);
      reconcile();
    },
    remove(space: string) {
      roots.delete(space);
      reconcile();
    },
    stop() {
      stopped = true;
      for (const group of groups.values()) for (const stop of group.stops) stop();
      groups.clear();
      roots.clear();
    },
  };
}
