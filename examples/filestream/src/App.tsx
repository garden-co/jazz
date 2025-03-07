import { FileWidget } from "./FileWidget.js";
import { Logo } from "./Logo.tsx";

function App() {
  return (
    <>
      <main className="container mt-16 flex flex-col gap-8">
        <Logo />
        <FileWidget />
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
