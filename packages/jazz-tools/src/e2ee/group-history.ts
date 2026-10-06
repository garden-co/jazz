import type { GroupMembership, GroupRoot, GroupSuccessor } from "./groups.js";

export type GroupHistoryScope = { groupIds: readonly string[] } | { accountId: string };

export interface GroupHistoryReader {
  roots(selector: { id: string } | { accountId: string }): Promise<GroupRoot[]>;
  members(
    selector: { groupId: string } | { memberKind: "account" | "group"; memberId: string },
  ): Promise<GroupMembership[]>;
  successors(groupId: string): Promise<GroupSuccessor[]>;
}

export type GroupHistory = {
  roots: GroupRoot[];
  records: GroupMembership[];
  successors: GroupSuccessor[];
};

/** Collect raw historical dependencies; only authenticated replay grants membership. */
export async function collectGroupHistory(
  scope: GroupHistoryScope,
  reader: GroupHistoryReader,
): Promise<GroupHistory> {
  const roots = new Map<string, GroupRoot>();
  const records = new Map<string, GroupMembership>();
  const successors = new Map<string, GroupSuccessor>();
  const visited = new Set<string>();
  let pending = new Set<string>();
  const enqueue = (id: string) => {
    if (!visited.has(id)) pending.add(id);
  };
  const includeMembers = (rows: GroupMembership[]) => {
    for (const row of rows) {
      records.set(row.id, row);
      enqueue(row.groupId);
      if (row.memberKind === "group") enqueue(row.memberId);
    }
  };

  if ("groupIds" in scope) {
    for (const id of scope.groupIds) enqueue(id);
  } else {
    const [created, memberships] = await Promise.all([
      reader.roots({ accountId: scope.accountId }),
      reader.members({ memberKind: "account", memberId: scope.accountId }),
    ]);
    for (const root of created) {
      roots.set(root.id, root);
      enqueue(root.id);
    }
    includeMembers(memberships);
  }

  while (pending.size) {
    const frontier = pending;
    pending = new Set();
    for (const id of frontier) visited.add(id);
    await Promise.all(
      [...frontier].map(async (id) => {
        // Empty incoming/outgoing predicates are part of the authoritative read
        // set too: a concurrent connecting edge must invalidate acceptance.
        const [found, outgoing, epochs, incoming] = await Promise.all([
          reader.roots({ id }),
          reader.members({ groupId: id }),
          reader.successors(id),
          reader.members({ memberKind: "group", memberId: id }),
        ]);
        for (const root of found) roots.set(root.id, root);
        for (const epoch of epochs) successors.set(epoch.id, epoch);
        // Removed and invalid candidates still connect historical dependencies.
        includeMembers(outgoing);
        includeMembers(incoming);
      }),
    );
  }
  return {
    roots: [...roots.values()],
    records: [...records.values()],
    successors: [...successors.values()],
  };
}
