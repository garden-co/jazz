import { useSuspenseAccount } from "jazz-tools/react";
import { CursorAccount } from "./schema";
import { Logo } from "./Logo";
import Container from "./components/Container";
import { getName } from "./utils/getName";

function App() {
  const me = useSuspenseAccount(CursorAccount, { resolve: { profile: true } });

  const profileName = me.$isLoaded ? me.profile.name : undefined;
  const sessionID = me.$isLoaded ? me.$jazz.sessionID : undefined;

  return (
    <>
      <main className="h-screen">
        <Container />
      </main>

      <footer className="fixed bottom-4 right-4 flex items-center gap-4">
        <input
          type="text"
          value={getName(profileName, sessionID)}
          onChange={(e) => {
            if (!me.$isLoaded) return;
            me.profile.$jazz.set("name", e.target.value);
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
