import { useAccount, useCoState } from "jazz-react";
import { ID } from "jazz-tools";
import { CursorFeed } from "../schema";
import { getColor } from "../utils/getColor.ts";
import { getName } from "../utils/getName";
import Canvas from "./Canvas";

const OLD_CURSOR_AGE_SECONDS = Number(
  import.meta.env.VITE_OLD_CURSOR_AGE_SECONDS,
);

function Avatar({
  name,
  color,
  active,
}: { name: string; color: string; active: boolean }) {
  return (
    <span
      title={name}
      className={[
        "size-6 text-xs font-medium uppercase bg-white inline-flex items-center justify-center rounded-full border-2",
        active ? "" : "opacity-50",
      ].join(" ")}
      style={{ color, borderColor: color }}
    >
      {name.replace("Anonymous ", "")[0]}
    </span>
  );
}

/** A higher order component that wraps the canvas. */
function Container({ cursorFeedID }: { cursorFeedID: ID<CursorFeed> }) {
  const { me } = useAccount();
  const cursors = useCoState(CursorFeed, cursorFeedID, { resolve: true });

  const remoteCursors = Object.values(cursors?.perSession ?? {})
    .map((entry) => ({
      entry,
      position: entry.value.position,
      color: getColor(entry.tx.sessionID),
      name: getName(entry.by?.profile?.name, entry.tx.sessionID),
      age: new Date().getTime() - new Date(entry.madeAt).getTime(),
      active:
        !OLD_CURSOR_AGE_SECONDS ||
        entry.madeAt >= new Date(Date.now() - 1000 * OLD_CURSOR_AGE_SECONDS),
      isMe: entry.tx.sessionID === me?.sessionID,
    }))
    .sort((a, b) => {
      return b.entry.madeAt.getTime() - a.entry.madeAt.getTime();
    });

  return (
    <>
      <div className="absolute top-4 right-4 bg-white p-2 rounded-lg shadow">
        <div className="flex items-center gap-1">
          {remoteCursors.slice(0, 5).map(({ name, color, entry, active }) => (
            <Avatar
              key={entry.tx.sessionID}
              name={name}
              color={color}
              active={active}
            />
          ))}
        </div>
      </div>

      <Canvas
        onCursorMove={(move) => {
          if (!(cursors && me)) return;

          cursors.push({
            position: {
              x: move.position.x,
              y: move.position.y,
            },
          });
        }}
        remoteCursors={remoteCursors}
        name={getName(me?.profile?.name, me?.sessionID)}
      />
    </>
  );
}

export default Container;
