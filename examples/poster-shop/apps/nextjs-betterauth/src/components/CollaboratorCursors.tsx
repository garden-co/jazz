"use client";

import { Avatar, AvatarGroup } from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { memo, useCallback, useEffect, useRef } from "react";
import { app } from "@/schema";
import { cursorColor, cursorColorKey } from "@/src/lib/poster";

/** At most one presence write per interval; the last position always lands. */
const CURSOR_WRITE_INTERVAL_MS = 50;

/**
 * Publishes the current user's pointer as their own `cursors` row. The row is
 * found or created once, then updated in place; nothing here reads shapes, so
 * a cursor write never re-runs the canvas shape query.
 */
export function useCursorPublisher(canvasId: string, author: string | null, name: string) {
  const db = useDb();
  const row = useRef<{ key: string; id: Promise<string | null> } | null>(null);
  const pending = useRef<{ x: number; y: number; active: boolean } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastWrite = useRef(0);

  const ensureRow = useCallback(
    (x: number, y: number) => {
      if (!author) return Promise.resolve(null);
      const key = `${canvasId}:${author}`;
      if (row.current?.key !== key) {
        const id = db
          .one(app.cursors.where({ canvasId, author }), { tier: "local-first-unless-empty" })
          .then((existing) => {
            if (existing) return existing.id;
            const { value } = db.insert(app.cursors, {
              canvasId,
              author,
              name,
              x,
              y,
              color: cursorColorKey(author),
              active: true,
            });
            return value.id;
          })
          .catch(() => null);
        row.current = { key, id };
      }
      return row.current.id;
    },
    [db, canvasId, author, name],
  );

  const flush = useCallback(() => {
    timer.current = null;
    const next = pending.current;
    pending.current = null;
    if (!next) return;
    lastWrite.current = Date.now();
    void ensureRow(next.x, next.y).then((id) => {
      if (id) db.update(app.cursors, id, { ...next, name });
    });
  }, [db, ensureRow, name]);

  const schedule = useCallback(
    (next: { x: number; y: number; active: boolean }) => {
      pending.current = next;
      if (timer.current) return;
      const wait = Math.max(0, CURSOR_WRITE_INTERVAL_MS - (Date.now() - lastWrite.current));
      timer.current = setTimeout(flush, wait);
    },
    [flush],
  );

  const last = useRef<{ x: number; y: number; active: boolean }>({ x: 0, y: 0, active: false });
  const move = useCallback(
    (x: number, y: number) => {
      last.current = { x, y, active: true };
      schedule(last.current);
    },
    [schedule],
  );
  const leave = useCallback(() => {
    if (!row.current || !last.current.active) return;
    last.current = { ...last.current, active: false };
    schedule(last.current);
  }, [schedule]);

  // Hide the cursor when the tab is closed or the poster changes.
  useEffect(() => {
    window.addEventListener("pagehide", leave);
    return () => {
      window.removeEventListener("pagehide", leave);
      leave();
    };
  }, [leave]);

  return { move, leave };
}

/**
 * Other members' live cursors, drawn in poster coordinates. This component
 * owns the only cursor subscription on the canvas, so cursor traffic
 * re-renders just this group.
 */
export const CursorLayer = memo(function CursorLayer({
  canvasId,
  author,
  unitsPerPixel,
}: {
  canvasId: string;
  author: string | null;
  unitsPerPixel: number;
}) {
  const { data: cursors = [] } = useAll(app.cursors.where({ canvasId, active: true }));
  const scale = unitsPerPixel;
  return (
    <g aria-hidden="true" pointerEvents="none" className="cursor-layer">
      {cursors
        .filter((cursor) => cursor.author !== author)
        .map((cursor) => (
          <g
            key={cursor.id}
            transform={`translate(${cursor.x} ${cursor.y}) scale(${scale})`}
            className="cursor"
            style={{ color: cursorColor(cursor.color) }}
          >
            <path
              d="M0 0 L0 17 L4.5 12.8 L7.6 19.6 L10.4 18.4 L7.4 11.8 L13 11.6 Z"
              fill="currentColor"
              stroke="var(--color-background-card)"
              strokeWidth={1.5}
              strokeLinejoin="round"
            />
            <g transform="translate(14 20)">
              <rect
                width={labelWidth(cursor.name)}
                height={20}
                className="cursor-label-background"
              />
              <text x={7} y={14} className="cursor-label">
                {cursor.name || "Collaborator"}
              </text>
            </g>
          </g>
        ))}
    </g>
  );
});

/** Rough label width in CSS pixels; the label is decoration for the arrow. */
function labelWidth(name: string) {
  return Math.max(4, (name || "Collaborator").length) * 7 + 14;
}

/** Who is on the poster right now, from the same presence rows. */
export function CollaboratorAvatars({
  canvasId,
  author,
}: {
  canvasId: string;
  author: string | null;
}) {
  const { data: cursors = [] } = useAll(
    app.cursors.where({ canvasId, active: true }).select("author", "name"),
  );
  const others = cursors.filter((cursor) => cursor.author !== author);
  if (others.length === 0) return null;
  return (
    <AvatarGroup size="sm">
      {others.map((cursor) => (
        <Avatar key={cursor.id} name={cursor.name || "Collaborator"} tooltip size="sm" />
      ))}
    </AvatarGroup>
  );
}
