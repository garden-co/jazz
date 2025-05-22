import { useAccount } from "jazz-react";
import { useEffect, useState } from "react";
import { Logo } from "./Logo";
import Container from "./components/Container";
import { getName } from "./utils/getName";
import { loadCursorContainer } from "./utils/loadCursorContainer";

const cursorFeedIDToLoad = import.meta.env.VITE_CURSOR_FEED_ID;
const groupIDToLoad = import.meta.env.VITE_GROUP_ID;

function App() {
  const { me } = useAccount();
  const [loaded, setLoaded] = useState(false);
  const [cursorFeedID, setCursorFeedID] = useState<string | null>(null);

  useEffect(() => {
    console.log("Loading cursor feed...", me?.id);
    if (!me?.id) return;
    const loadCursorFeed = async () => {
      const id = await loadCursorContainer(
        me,
        cursorFeedIDToLoad,
        groupIDToLoad,
      );
      if (id) {
        setCursorFeedID(id);
        setLoaded(true);
      }
    };
    loadCursorFeed();
  }, [me?.id]);

  return (
    <>
      <main className="h-screen">
        {loaded && cursorFeedID ? (
          <Container cursorFeedID={cursorFeedID} />
        ) : (
          <div>Loading...</div>
        )}
      </main>

      <footer className="fixed bottom-4 right-4 flex items-center gap-4">
        <input
          type="text"
          value={getName(me?.profile?.name, me?.sessionID)}
          onChange={(e) => {
            if (!me?.profile) return;
            me.profile.name = e.target.value;
          }}
          placeholder="Your name"
          className="px-2 py-1 rounded border pointer-events-auto"
          autoComplete="off"
          maxLength={32}
        />
        <div className="pointer-events-none">
          <Logo />
        </div>
      </footer>
    </>
  );
}

export default App;
