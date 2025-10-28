import { CoRichText } from "jazz-tools";
import { useAccount, useIsAuthenticated } from "jazz-tools/react";
import { useMemo } from "react";
import { Logo } from "./app-logo.tsx";
import { AuthButton } from "./auth-button.tsx";
import Editor from "./editor/editor.tsx";
import { JazzAccount } from "./schema.ts";
import Textarea from "./textarea.tsx";

function App() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: true, root: true },
  });

  const isAuthenticated = useIsAuthenticated();

  const memoCoRichText: CoRichText | undefined = useMemo(() => {
    console.log("memoCoRichText");
    return me?.profile.bio ?? undefined;
  }, [me?.$jazz.id, me?.profile.bio?.$jazz.id]);
  // Only recreate if the account or the bio changes
  // https://github.com/garden-co/jazz/blob/b4cd307ebac5860df2f83d75a915906f472a5cd4/examples/richtext/src/Editor.tsx#L46C1-L46C88

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

        <div className="flex flex-col gap-4">
          <Editor coRichText={memoCoRichText} />
          <Editor coRichText={memoCoRichText} />
          <Textarea coRichText={me?.profile.bio} />
        </div>
      </main>
    </>
  );
}

export default App;
