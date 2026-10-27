import { expect, it } from "vitest";
import { groupQueryDependencies, type DependencyWatch } from "./group-query-dependencies.js";
import { deviceRequestSchema } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { schema as s } from "../schema-namespace.js";

// Capture the actual managed-table queries: no storage or cryptographic result
// can establish that unrelated groups were never subscribed to.
function fixture() {
  const active = new Map<string, { emit: (rows: { id: string }[]) => void; stop: () => void }>();
  const queries: { table: string; conditions: { column: string; value: unknown }[] }[] = [];
  const watch: DependencyWatch = (query, onRows) => {
    const shape = JSON.parse(query._build()) as (typeof queries)[number];
    queries.push(shape);
    const column =
      shape.table === "__e2ee_groups"
        ? "id"
        : shape.conditions.some((c) => c.column === "groupId")
          ? "groupId"
          : "memberId";
    const id = String(shape.conditions.find((c) => c.column === column)?.value);
    expect(id).not.toBe("undefined");
    const key = `${column}:${id}`;
    const stop = () => {
      active.delete(key);
    };
    active.set(key, { emit: onRows ?? (() => {}), stop });
    return stop;
  };
  const graph = groupQueryDependencies(
    s.defineApp({ ...deviceRequestSchema, ...groupSchema }).wasmSchema,
    watch,
  );
  const members = (
    column: string,
    id: string,
    rows: { id: string; groupId: string; memberKind: string; memberId: string }[],
  ) => active.get(`${column}:${id}`)!.emit(rows);
  const ids = () => [...active.keys()].filter((key) => key.startsWith("id:")).sort();
  return { graph, members, ids, active, queries };
}

it("watches no groups for account-only grants, then follows incoming and outgoing candidates", () => {
  const f = fixture();
  f.graph.update("alice-space", []);
  expect(f.queries).toEqual([]);
  f.graph.update("alice-space", ["team"]);
  expect(f.ids()).toEqual(["id:team"]);
  f.members("groupId", "team", [
    { id: "edge-1", groupId: "team", memberKind: "group", memberId: "child" },
  ]);
  f.members("memberId", "team", [
    { id: "edge-2", groupId: "parent", memberKind: "group", memberId: "team" },
  ]);
  expect(f.ids()).toEqual(["id:child", "id:parent", "id:team"]);
  // A cycle must not create duplicate subscriptions or keep a disconnected
  // component alive once Alice's last space stops needing it.
  f.members("groupId", "child", [
    { id: "edge-3", groupId: "child", memberKind: "group", memberId: "team" },
  ]);
  expect(f.ids()).toHaveLength(3);
  f.graph.remove("alice-space");
  expect(f.active.size).toBe(0);
});

it("keeps a shared component until the last space releases it and ignores late callbacks", () => {
  const f = fixture();
  f.graph.update("alice-space", ["team"]);
  f.graph.update("bob-space", ["team"]);
  expect(f.queries).toHaveLength(3);
  const late = f.active.get("groupId:team")!.emit;
  f.graph.remove("alice-space");
  expect(f.ids()).toEqual(["id:team"]);
  f.graph.stop();
  late([{ id: "late" }]);
  expect(f.active.size).toBe(0);
  expect(f.queries).toHaveLength(3);
});
