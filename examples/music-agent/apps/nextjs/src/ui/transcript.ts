/** The fields branch navigation needs; rows from `app.turns` satisfy it. */
export type TurnNode = {
  id: string;
  parentId?: string | null;
  role: "user" | "assistant";
  $createdAt: Date | string | number;
};

const time = (turn: TurnNode) => new Date(turn.$createdAt).getTime();

/** The turns on the branch ending at `headId`, oldest first. */
export function branchPath<T extends TurnNode>(
  turns: readonly T[],
  headId: string | undefined | null,
): T[] {
  const byId = new Map(turns.map((turn) => [turn.id, turn]));
  const path: T[] = [];
  const seen = new Set<string>();
  for (let id = headId; id && !seen.has(id); id = byId.get(id)?.parentId) {
    seen.add(id);
    const turn = byId.get(id);
    if (!turn) break;
    path.unshift(turn);
  }
  return path;
}

/** Alternatives for a turn: turns with the same parent and role, oldest first. */
export function siblingsOf<T extends TurnNode>(turns: readonly T[], turn: T): T[] {
  return turns
    .filter(
      (other) => (other.parentId ?? null) === (turn.parentId ?? null) && other.role === turn.role,
    )
    .sort((a, b) => time(a) - time(b));
}

/** The newest leaf under `turnId`: where the conversation continues on that branch. */
export function latestLeaf<T extends TurnNode>(turns: readonly T[], turnId: string): string {
  let current = turnId;
  const seen = new Set<string>();
  for (;;) {
    seen.add(current);
    const children = turns.filter((turn) => turn.parentId === current && !seen.has(turn.id));
    if (!children.length) return current;
    current = children.reduce((newest, child) => (time(child) > time(newest) ? child : newest)).id;
  }
}
