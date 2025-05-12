import { useAccount, useIsAuthenticated } from "jazz-react";
import { AuthButton } from "./AuthButton.tsx";
import { Editor } from "./Editor.tsx";
import { Logo } from "./Logo.tsx";

function App() {
  const { me } = useAccount({
    resolve: { profile: { bio: true }, root: true },
  });

  const isAuthenticated = useIsAuthenticated();

  return (
    <>
      <header>
        <nav className="container flex justify-between items-center py-3">
          {isAuthenticated ? (
            <span>You're logged in.</span>
          ) : (
            <span>Authenticate to share the data with another device.</span>
          )}
          <AuthButton />
        </nav>
      </header>
      <main className="container mt-16 flex flex-col gap-8">
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
