"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import type { TextAnchor, TextRange } from "jazz-tools";
import {
  capturePresence,
  samePresence,
  type Collaborator,
  type SelectionPresence,
} from "jazz-tools/prosemirror";
import type { EditorState } from "prosemirror-state";
import { app } from "@/schema";
import { memberName, useWorkspace } from "./workspace-context";

/** How often a write follows the cursor, and how long a quiet editor counts as present. */
const PRESENCE_WRITE_MS = 150;
const HEARTBEAT_MS = 10_000;
const STALE_MS = 30_000;

/** Theme colours collaborators are told apart by. */
const COLORS = ["blue", "purple", "orange", "green", "pink", "teal", "red", "cyan"];

function colorFor(account: string): string {
  let hash = 0;
  for (const char of account) hash = (hash * 31 + char.charCodeAt(0)) | 0;
  return `var(--color-icon-${COLORS[Math.abs(hash) % COLORS.length]})`;
}

// #region rich-text-presence
/**
 * Who else has this song sheet open, and where their cursors are. Each open
 * editor keeps one presence row: written as the selection moves (at most
 * every 150 ms), refreshed every 10 s while it stays open and deleted when it
 * closes. A row this client has not seen refreshed for 30 s belongs to an
 * editor that went away; timing it by this client's clock, not the writer's,
 * keeps a wrong clock elsewhere from hiding or keeping anyone. Names and
 * colours come from the row's account, never from what the row says.
 */
export function useSheetPresence(pageId: string) {
  const db = useDb();
  const { me, members, workspace } = useWorkspace();
  const rowId = useRef<string | null>(null);
  const last = useRef<SelectionPresence | undefined>(undefined);
  const pending = useRef<SelectionPresence | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [now, setNow] = useState(() => Date.now());

  const write = (presence: SelectionPresence) => {
    const values = {
      cursor: presence.cursor,
      selection: presence.selection ?? null,
      seenAt: Date.now(),
    };
    if (rowId.current) {
      db.update(app.sheetPresence, rowId.current, values);
    } else {
      rowId.current = db.insert(app.sheetPresence, {
        workspaceId: workspace.id,
        pageId,
        account: me,
        ...values,
      }).value.id;
    }
    last.current = presence;
  };

  useEffect(() => {
    const heartbeat = setInterval(() => {
      setNow(Date.now());
      if (rowId.current) db.update(app.sheetPresence, rowId.current, { seenAt: Date.now() });
    }, HEARTBEAT_MS);
    return () => {
      clearInterval(heartbeat);
      if (timer.current) clearTimeout(timer.current);
      if (rowId.current) db.delete(app.sheetPresence, rowId.current);
      rowId.current = null;
    };
  }, [db, pageId]);

  /** Call with the editor's state after each selection or document change. */
  const report = (state: EditorState) => {
    const presence = capturePresence(state, db.textAnchors());
    if (samePresence(last.current, presence)) return;
    pending.current = presence;
    if (timer.current) return;
    write(presence);
    pending.current = null;
    timer.current = setTimeout(function flush() {
      timer.current = null;
      if (pending.current) {
        write(pending.current);
        pending.current = null;
        timer.current = setTimeout(flush, PRESENCE_WRITE_MS);
      }
    }, PRESENCE_WRITE_MS);
  };

  const { data: rows } = useAll(app.sheetPresence.where({ pageId }));

  // Rows this editor left behind (a closed laptop, a crashed tab) were
  // timed by this clock, so they can be told apart from a live tab's.
  useEffect(() => {
    for (const row of rows ?? []) {
      if (row.account === me && row.id !== rowId.current && Date.now() - row.seenAt > STALE_MS) {
        db.delete(app.sheetPresence, row.id);
      }
    }
  }, [rows, me, db]);

  // When this client last saw each row change.
  const seen = useRef(new Map<string, { seenAt: number; at: number }>());
  const collaborators = useMemo<Collaborator[]>(() => {
    const live: Collaborator[] = [];
    for (const row of rows ?? []) {
      const last = seen.current.get(row.id);
      if (!last || last.seenAt !== row.seenAt) {
        seen.current.set(row.id, { seenAt: row.seenAt, at: Date.now() });
      }
      if (row.id === rowId.current || now - seen.current.get(row.id)!.at > STALE_MS) continue;
      live.push({
        id: row.id,
        name: memberName(members, row.account),
        color: colorFor(row.account),
        cursor: row.cursor as unknown as TextAnchor,
        selection: (row.selection ?? undefined) as unknown as TextRange | undefined,
      });
    }
    return live;
  }, [rows, now, members]);
  return { collaborators, report };
}
// #endregion rich-text-presence
