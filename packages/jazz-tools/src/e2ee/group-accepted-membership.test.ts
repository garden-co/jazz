import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { AcceptedRows } from "../runtime/accepted-read.js";
import type { Db, QueryBuilder } from "../runtime/db.js";
import type { AcceptedDeviceTransactionScope } from "./accepted-membership.js";
import { deviceRequestSchema } from "./device-requests.js";
import { Groups } from "./group-lifecycle.js";
import { groupSchema, type GroupMembership, type GroupRoot } from "./groups.js";
import type { PublicMembershipHistory } from "./public-membership.js";
import type { DeviceSigner, KeyEnvelope } from "./types.js";

// Adapter assembly controls, not cryptographic authority or commit-time validation.
const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema }, { migrations: "none" });
const account = "account";
const id = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
const root = { id: id(1), accountId: account } as GroupRoot;
const edge = {
  id: id(1),
  groupId: id(1),
  memberId: id(2),
  memberKind: "group",
  authorAccountId: account,
  operation: "add",
} as GroupMembership;

function response<T extends { id: string }>(
  rows: T[],
  position = "9007199254740999",
): AcceptedRows<T> {
  return {
    rows,
    schema: "fixture",
    through: "18446744073709551615",
    evidence: rows.map((row) => ({
      rowId: row.id,
      state: { transactionId: "restored", position: "9007199254741010" },
      content: { transactionId: "signed", position },
    })),
  };
}

const ownHistory: PublicMembershipHistory = {
  roots: response([]),
  keys: response([]),
  approvals: response([]),
  successors: response([]),
  recovery: response([]),
};

function fixture(read: (query: QueryBuilder<unknown>) => AcceptedRows<unknown>) {
  const calls: string[] = [];
  const forbidden = () => {
    throw new Error("Replica/cache access is not authority");
  };
  const tx: AcceptedDeviceTransactionScope = {
    kind: "serializable",
    all: forbidden,
    one: forbidden,
    insert: forbidden,
    upsert: forbidden,
    async readAccepted<T>(query: QueryBuilder<T>) {
      calls.push(query._table);
      return read(query) as AcceptedRows<T>;
    },
  };
  const groups = new Groups(
    {} as Db,
    account,
    "fixture",
    app,
    {} as KeyEnvelope,
    {} as DeviceSigner,
    () => {},
    (value) => value,
  );
  return {
    calls,
    run: (ids?: string[]) =>
      groups.readMembership(
        tx,
        ids === undefined ? { accountId: account } : { groupIds: ids },
        ownHistory,
      ),
  };
}

it("retains covered-empty account predicates instead of using replica rows", async () => {
  const f = fixture(() => response([]));
  const snapshot = await f.run();
  expect(f.calls).toEqual(["__e2ee_groups", "__e2ee_group_membership"]);
  expect(snapshot.roots.rows).toEqual([]);
  expect(snapshot.records.contentOrigins).toEqual([]);
});

it("keeps empty incoming/outgoing and root/successor predicates in every 128-ID chunk", async () => {
  const f = fixture(() => response([]));
  await f.run(Array.from({ length: 129 }, (_, index) => id(index + 1)));
  expect(f.calls.filter((table) => table === "__e2ee_group_membership")).toHaveLength(4);
  expect(f.calls.filter((table) => table === "__e2ee_groups")).toHaveLength(2);
  expect(f.calls.filter((table) => table === "__e2ee_group_successors")).toHaveLength(2);
});

it.each(["missing", "misassociated", "overflow", "empty-cut"])(
  "refuses %s paired coverage at the group consumer boundary",
  async (fault) => {
    const f = fixture((query) => {
      if (query._table !== "__e2ee_groups") return response([]);
      const value = response([root]);
      if (fault === "missing") value.evidence = [];
      if (fault === "misassociated") value.evidence[0]!.rowId = id(99);
      if (fault === "overflow") value.evidence[0]!.state.position = "18446744073709551616";
      if (fault === "empty-cut") value.through = "";
      return value;
    });
    await expect(f.run()).rejects.toThrow(/coverage|authority/);
  },
);

it.each(["identical", "payload", "evidence"])(
  "checks %s overlapping membership responses before deduplication",
  async (mode) => {
    let count = 0;
    const f = fixture((query) => {
      if (query._table !== "__e2ee_group_membership") return response([]);
      const value = response([{ ...edge }]);
      if (count++ && mode === "payload") value.rows[0]!.operation = "remove";
      if (count > 1 && mode === "evidence")
        value.evidence[0]!.state.transactionId = "different-restore";
      return value;
    });
    const pending = f.run([id(1), id(2)]);
    if (mode === "identical") {
      const snapshot = await pending;
      expect(snapshot.records.rows).toHaveLength(1);
      expect(snapshot.records.contentOrigins).toHaveLength(1);
    } else await expect(pending).rejects.toThrow(/Conflicting E2EE group authority/);
  },
);

it("preserves exact content order independently of later state and per-table row IDs", async () => {
  const f = fixture((query) => {
    if (query._table === "__e2ee_groups") return response([root], "9007199254740993");
    if (query._table === "__e2ee_group_membership") return response([edge], "9007199254740999");
    return response([]);
  });
  const snapshot = await f.run([id(1), id(2)]);
  expect(snapshot.roots.contentOrigins[0]!.position).toBe("9007199254740993");
  expect(snapshot.records.contentOrigins[0]!.position).toBe("9007199254740999");
  expect(snapshot.records.contentOrigins[0]!.state.position).toBe("9007199254741010");
  expect(snapshot.roots.rows[0]!.id).toBe(snapshot.records.rows[0]!.id);
});
