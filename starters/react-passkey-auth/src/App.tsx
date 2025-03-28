import { useAccount, useIsAuthenticated } from "jazz-react";
import { AuthButton } from "./AuthButton.tsx";
import { Form } from "./Form.tsx";
import { Logo } from "./Logo.tsx";

function App() {
  const { me } = useAccount({ resolve: { profile: true, root: true } });

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
          {!!me?.root.age && (
            <p>As of today, you are {me.root.age} years old.</p>
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
          <a
            className="font-semibold underline"
            href="https://jazz.tools/docs/react/guide"
          >
            jazz.tools/docs/react/guide
          </a>{" "}
          for a full tutorial.
        </p>
      </main>
    </>
  );
}

export default App;
