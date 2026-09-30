"use client";

import { useEffect, useState } from "react";
import type { Db, PermissionAdvice } from "jazz-tools";
import { useDb } from "jazz-tools/react";
import { useWorkspace } from "./workspace-context";

/**
 * Whether to offer an action, from core's permission advice (`db.canInsert`,
 * `db.canUpdate`, `db.canDelete`), which evaluates permissions.ts itself: the
 * UI keeps no copy of the rules. Advice is not enforcement; the authority still
 * decides every write.
 *
 * Returns `undefined` until advice arrives, so a control can stay hidden
 * without claiming it is denied. It is offered unless the advice is
 * `"denied"`: `"unknown"` means Jazz could not tell (for example while
 * offline), so the action is offered and the authority decides. Advice is
 * asked again when `key` changes or the viewer's role or grants change.
 */
export function useCan(
  check: (db: Db) => Promise<PermissionAdvice>,
  key: string,
): boolean | undefined {
  const db = useDb();
  const { accessVersion } = useWorkspace();
  const fullKey = `${accessVersion}|${key}`;
  const [advice, setAdvice] = useState<{ key: string; allowed: boolean } | null>(null);

  useEffect(() => {
    let current = true;
    check(db).then(
      (result) => current && setAdvice({ key: fullKey, allowed: result !== "denied" }),
      () => current && setAdvice({ key: fullKey, allowed: false }),
    );
    return () => {
      current = false;
    };
    // `check` is a fresh closure every render; `fullKey` names what it asks.
  }, [db, fullKey]);

  return advice?.key === fullKey ? advice.allowed : undefined;
}
