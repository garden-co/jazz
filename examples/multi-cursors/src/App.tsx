import Canvas from "./Canvas";
import { Logo } from "./Logo";

function App() {
  return (
    <>
      <main className="h-screen">
        <Canvas />
      </main>
      <footer className="fixed bottom-4 right-4">
        <Logo />
      </footer>
    </>
  );
}

export default App;
