import { useAccount, useIsAuthenticated } from "jazz-tools/react";
import { AuthButton } from "./AuthButton.tsx";
import { Form } from "./Form.tsx";
import { Logo } from "./Logo.tsx";
import { JazzAccount, getUserAge } from "./schema.ts";

function App() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: true, root: true },
  });

  const isAuthenticated = useIsAuthenticated();

  return (
    <>
      <header>
        <nav className="max-w-2xl mx-auto flex justify-between items-center p-3">
          {isAuthenticated ? (
            <span>You're logged in.</span>
          ) : (
            <span>Authenticate to share the data with another device.</span>
          )}
          <AuthButton />
        </nav>
      </header>
      <main className="max-w-2xl mx-auto px-3 mt-16 flex flex-col gap-8">
        <Logo />

        <div className="text-center">
          <h1>
            Welcome{me?.profile.firstName ? <>, {me?.profile.firstName}</> : ""}
            !
          </h1>
          {!!me?.root && (
            <p>As of today, you are {getUserAge(me.root)} years old.</p>
          )}
        </div>

        <Form />

        <p className="text-center">
          Edit the form above,{" "}
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="font-semibold underline"
          >
            refresh
          </button>{" "}
          this page, and see your changes persist.
        </p>

        <p className="text-center">
          Edit <code className="font-semibold">schema.ts</code> to add more
          fields.
        </p>

        <p className="text-center my-16">
          Go to{" "}
          <a className="font-semibold underline" href="https://jazz.tools/docs">
            jazz.tools/docs
          </a>{" "}
          for our docs.
        </p>
      </main>
    </>
  );
}

export default App;
