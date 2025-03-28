import { useAccount, useCoState } from "jazz-react";
import { ID } from "jazz-tools";
import { CursorFeed } from "../schema";
import { getName } from "../utils/getName";
import Canvas from "./Canvas";

/** A higher order component that wraps the canvas. */
function Container({ cursorFeedID }: { cursorFeedID: ID<CursorFeed> }) {
  const { me } = useAccount();
  const cursors = useCoState(CursorFeed, cursorFeedID, { resolve: true });

  return (
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
      remoteCursors={Object.values(cursors?.perSession ?? {})}
      name={getName(me?.profile?.name, me?.sessionID)}
    />
  );
}

export default Container;
