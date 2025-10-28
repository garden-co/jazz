import { useAccount, useIsAuthenticated } from "jazz-tools/react";
import { AuthButton } from "./AuthButton.tsx";
import { Editor } from "./Editor.tsx";
import { Logo } from "./Logo.tsx";
import { JazzAccount } from "./schema.ts";

function App() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: { bio: true }, root: true },
  });

  const isAuthenticated = useIsAuthenticated();

  return (
    <>
      <header>
        <nav className="max-w-3xl mx-auto px-3 flex justify-between items-center py-3">
          {isAuthenticated ? (
            <span>You're logged in.</span>
          ) : (
            <span>Authenticate to share the data with another device.</span>
          )}
          <AuthButton />
        </nav>
      </header>
      <main className="max-w-3xl mx-auto px-3 mt-16 flex flex-col gap-8">
        <Logo />

        <div className="text-center">
          <h1>
            Welcome{me?.profile.firstName ? <>, {me?.profile.firstName}</> : ""}
            !
          </h1>
        </div>

        {me ? <Editor value={me.profile.bio} /> : null}
      </main>
    </>
  );
}

export default App;
