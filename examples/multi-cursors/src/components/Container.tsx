import { useAccount, useCoState } from "jazz-tools/react";
import { CursorFeed } from "../schema";
import { getColor } from "../utils/getColor.ts";
import { getName } from "../utils/getName";
import Canvas from "./Canvas";
import { useSyncConnectionStatus } from "jazz-tools/react-core";

const OLD_CURSOR_AGE_SECONDS = Number(
  import.meta.env.VITE_OLD_CURSOR_AGE_SECONDS,
);

function Avatar({
  name,
  color,
  active,
}: {
  name: string;
  color: string;
  active: boolean;
}) {
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
function Container({ cursorFeedID }: { cursorFeedID: string }) {
  const { me } = useAccount();
  const cursors = useCoState(CursorFeed, cursorFeedID, { resolve: true });

  const connected = useSyncConnectionStatus();

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
      isMe: entry.tx.sessionID === me?.$jazz.sessionID,
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
      {!connected && (
        <div className="absolute top-16 right-4 bg-white p-2 rounded-lg shadow">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 border-2 border-gray-400 border-t-transparent rounded-full animate-spin"></div>
            <span className="text-sm text-gray-700">Connecting...</span>
          </div>
        </div>
      )}

      <Canvas
        onCursorMove={(move) => {
          if (!(cursors && me)) return;

          cursors.$jazz.push({
            position: {
              x: move.position.x,
              y: move.position.y,
            },
          });
        }}
        remoteCursors={remoteCursors}
        name={getName(me?.profile?.name, me?.$jazz.sessionID)}
      />
    </>
  );
}

export default Container;
