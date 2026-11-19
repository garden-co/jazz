/**
 * Which spaces an app is using: the runtime keeps a watch open for the spaces
 * in active use only (the members and keys of a space it has no subscription
 * on are looked at when the pass runs at its natural moments). A subscription
 * on an encrypted table uses the space of each row it delivers, from the
 * first row until the subscription ends; the same space under several
 * subscriptions is one watch, which the runtime counts and releases with the
 * last.
 */
import type { RuntimeSubscriptionDelta } from "../drivers/types.js";

/** The native calls a watch is opened and closed with. */
export interface SpaceUseTarget {
  encryptionUseSpace(table: string, id: string, open: boolean): void;
  /** `{ table: { scope, column } }` as JSON: where each encrypted table keeps its space. */
  encryptionSpaces(): string;
}

type SpaceColumns = Record<string, { scope: string; column: string }>;

/** One subscription's use of spaces; `release` ends all of it. */
export interface SubscriptionSpaceUse {
  observe(delta: RuntimeSubscriptionDelta): void;
  release(): void;
}

export class SpaceUse {
  private readonly where: SpaceColumns;

  constructor(private readonly target: SpaceUseTarget) {
    this.where = JSON.parse(target.encryptionSpaces()) as SpaceColumns;
  }

  /** The use a subscription on `table` makes, or `null` if the table is not encrypted. */
  forSubscription(
    table: string,
    columns: readonly { name: string }[],
  ): SubscriptionSpaceUse | null {
    const entry = this.where[table];
    if (!entry) return null;
    const index = entry.column === "id" ? -1 : columns.findIndex((c) => c.name === entry.column);
    // A query that leaves the space column out cannot name its spaces.
    if (entry.column !== "id" && index < 0) return null;
    const mine = new Set<string>();
    let released = false;
    const note = (row: { id: string; values: readonly unknown[] } | undefined) => {
      if (!row) return;
      const value =
        index < 0 ? row.id : (row.values[index] as { value?: unknown } | undefined)?.value;
      // A delta that arrives after the subscription ended takes nothing.
      if (released || typeof value !== "string" || mine.has(value)) return;
      mine.add(value);
      this.take(entry.scope, value);
    };
    return {
      observe: (delta) => {
        for (const added of delta.added) note(added.row);
        for (const updated of delta.updated) note(updated.row);
      },
      release: () => {
        released = true;
        for (const id of mine) this.drop(entry.scope, id);
        mine.clear();
      },
    };
  }

  private take(scope: string, id: string) {
    this.target.encryptionUseSpace(scope, id, true);
  }

  private drop(scope: string, id: string) {
    this.target.encryptionUseSpace(scope, id, false);
  }
}
