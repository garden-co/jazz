// Subscription result-set and wire-update delivery helpers.
import { SortedMap, SortedSet } from "../../support/collections.js";
import {
  type ResultMemberEntry,
  type SyncMessage,
  type VersionBundle,
  type VersionCarrier,
  cmpResultMemberEntry,
  resultMemberEquals,
  resultMemberKey,
  expandVersionCarriers,
  rowKeyString,
  cmpTxId,
} from "../../_extern/node-policy-peer.js";
import type { MemberIndexKey, PeerSubscriptionState, RowKey } from "./subscription_state.js";

function memberRowKey(member: ResultMemberEntry): RowKey | undefined {
  const row = member.asRealRow();
  if (!row) return undefined;
  const occurrence = member.outputOccurrenceId();
  if (occurrence === undefined) return undefined;
  return [occurrence, row.table, row.rowUuid];
}

export function memberIndexKey(member: ResultMemberEntry): MemberIndexKey {
  const row = memberRowKey(member);
  if (row) return `r:${rowKeyString(row)}`;
  if (member.kind === "Synthetic") return `s:${member.table}:${member.row.join(",")}`;
  return `m:${resultMemberKey(member)}`;
}

export function resultMemberReplaces(
  replacement: ResultMemberEntry,
  previous: ResultMemberEntry,
): boolean {
  if (
    memberIndexKey(replacement) !== memberIndexKey(previous) ||
    resultMemberEquals(replacement, previous)
  )
    return false;
  const a = replacement.asRow();
  const b = previous.asRow();
  if (a && b) return a[0] === b[0] && a[1] === b[1];
  return replacement.kind === "Synthetic";
}

export function replacementRemovals(
  previous: PeerSubscriptionState,
  additions: ResultMemberEntry[],
): ResultMemberEntry[] {
  const out = new SortedSet<ResultMemberEntry>(cmpResultMemberEntry);
  for (const added of additions) {
    const old = previous.memberIndex.get(memberIndexKey(added))?.member;
    if (old && resultMemberReplaces(added, old)) out.add(old);
  }
  return out.toArray();
}

export function applyContributionAdd(
  state: PeerSubscriptionState,
  contribution: Iterable<ResultMemberEntry>,
  adds: ResultMemberEntry[],
  removes: ResultMemberEntry[],
): void {
  for (const member of contribution) {
    const key = memberIndexKey(member);
    const slot = state.memberIndex.get(key);
    if (!slot) {
      state.memberIndex.set(key, { member, refcount: 1 });
      adds.push(member);
      state.resultMemberSet.add(member);
    } else if (resultMemberEquals(slot.member, member)) {
      slot.refcount += 1;
    } else if (resultMemberReplaces(member, slot.member)) {
      const old = slot.member;
      removes.push(old);
      adds.push(member);
      state.resultMemberSet.delete(old);
      state.resultMemberSet.add(member);
      slot.member = member;
    } else {
      slot.refcount += 1;
    }
  }
}

export function applyContributionRemove(
  state: PeerSubscriptionState,
  contribution: Iterable<ResultMemberEntry>,
  removes: ResultMemberEntry[],
): void {
  for (const member of contribution) {
    const key = memberIndexKey(member);
    const slot = state.memberIndex.get(key);
    if (!slot) continue;
    if (slot.refcount > 1) {
      slot.refcount -= 1;
    } else {
      state.memberIndex.delete(key);
      removes.push(slot.member);
      state.resultMemberSet.delete(slot.member);
    }
  }
}

export function duplicatePhysicalRowResultSet(
  resultSet: SortedSet<ResultMemberEntry>,
): [RowKey, any, any] | undefined {
  const rows = new Map<string, any>();
  for (const member of resultSet) {
    const rowKey = memberRowKey(member);
    if (!rowKey) continue;
    const row = member.asRow();
    if (!row) continue;
    const k = rowKeyString(rowKey);
    const first = rows.get(k);
    rows.set(k, row[2]);
    if (first !== undefined) return [rowKey, first, row[2]];
  }
  return undefined;
}

export const bundleContainsCompleteTxPayload = (bundle: VersionBundle): boolean =>
  bundle.scope === "CompleteTransaction";

export function viewUpdateSingletonBundles(carriers: VersionCarrier[]): VersionBundle[] {
  try {
    return expandVersionCarriers(carriers);
  } catch {
    return [];
  }
}

export function storageReadMetricsBuckets(metrics: any): string {
  return (
    [
      "history_rows",
      "history_indexes",
      "ahead_current_rows",
      "global_current_rows",
      "global_current_indexes",
      "register_global_current_rows",
      "global_changes_rows",
      "global_changes_indexes",
      "transactions_rows",
      "transactions_indexes",
      "other",
    ] as const
  )
    .map((name) => {
      const bucket = metrics[name];
      return `${name}.reads=${bucket.reads}:${name}.ranges=${bucket.ranges}`;
    })
    .join(",");
}

export function bindingValuesInParamOrder(shape: any, binding: any): any[] {
  return [...shape.params().keys()].map((name: string) => {
    const v = binding.values().get(name);
    if (v === undefined) throw new Error("validated binding contains every shape param");
    return v;
  });
}

export const txIdSet = () => new SortedSet<any>(cmpTxId);
export const memberSet = (items?: Iterable<ResultMemberEntry>) =>
  new SortedSet<ResultMemberEntry>(cmpResultMemberEntry, items);
export const memberStateMap = () =>
  new SortedMap<ResultMemberEntry, [boolean, boolean]>(cmpResultMemberEntry);
