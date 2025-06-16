import { FileWidget } from "./FileWidget.js";
import { Logo } from "./Logo.tsx";

function App() {
  return (
    <>
      <main className="max-w-3xl mx-auto px-3 mt-16 flex flex-col gap-8">
        <Logo />
        <FileWidget />
      </main>
    </>
  );
}

export default App;
