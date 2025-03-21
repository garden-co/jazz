import { useAccount } from "jazz-react";
import { Group } from "jazz-tools";
import { ID } from "jazz-tools";
import { useEffect, useState } from "react";
import { Logo } from "./Logo";
import Canvas from "./components/Canvas";
import { Cursor, CursorContainer, CursorFeed, Vec2 } from "./schema";

function App() {
  const { me } = useAccount();
  const [cursorFeed, setCursorFeed] = useState<CursorFeed | null>(null);

  async function loadCursorContainer() {
    const group = await Group.load(
      "co_zLBGe35kD91bKhmtpjjDeo9VQYc" as ID<Group>,
      {},
    );
    if (group === undefined) return;
    group?.addMember("everyone", "reader");

    const cursorContainerID = CursorContainer.findUnique(
      "multi-cursors",
      group?.id as ID<Group>,
    );
    const cursorContainer = await CursorContainer.load(
      cursorContainerID as ID<CursorContainer>,
      {},
    );
    if (cursorContainer === undefined) {
      console.log("Global cursors does not exist, creating...");
      const cursorContainer = CursorContainer.create(
        {
          cursorFeed: CursorFeed.create([], {
            owner: group,
          }),
        },
        {
          owner: group,
          unique: "multi-cursors",
        },
      );
      console.log("Created global cursors", cursorContainer.id);
    } else {
      console.log("Global cursors already exists, loading...");
      setCursorFeed(cursorContainer.cursorFeed);

      if (me.sessionID) {
        const unsubscribe = cursorContainer.cursorFeed?.subscribe(
          [{}],
          (feed) => {
            console.log(feed);
            console.log(feed[me.id]);
            console.log(feed?.perSession[me.sessionID]);
          },
        );
      }
    }
  }

  useEffect(() => {
    loadCursorContainer();
  }, []);

  return (
    <>
      <main className="h-screen">
        <Canvas
          onCursorMove={(_move) => {
            const { x, y } = _move.position;
            cursorFeed?.push(
              Cursor.create(
                {
                  position: Vec2.create({ x, y }),
                },
                {
                  owner: cursorFeed._owner,
                },
              ),
            );
          }}
          remoteCursors={[
            {
              id: "1",
              position: { x: -40, y: 60 },
              color: "#dd0000",
              isDragging: false,
            },
            {
              id: "2",
              position: { x: 40, y: 80 },
              color: "#00dd00",
              isDragging: false,
            },
            {
              id: "3",
              position: { x: -10, y: 120 },
              color: "#0000dd",
              isDragging: false,
            },
          ]}
        />
      </main>

      <footer className="fixed bottom-4 right-4 pointer-events-none">
        <Logo />
      </footer>
    </>
  );
}

export default App;
