import Canvas from "./components/Canvas";
import { Logo } from "./Logo";

function App() {
  return (
    <>
      <main className="h-screen">
        <Canvas
          userCursors={[
            {
              id: "1",
              position: { x: -40, y: 60 },
              color: "#dd0000",
              isDragging: false,
            },
            {
              id: "2",
              position: { x: 40, y: 80 },
              color: "#00dd00",
              isDragging: false,
            },
            {
              id: "3",
              position: { x: -10, y: 120 },
              color: "#0000dd",
              isDragging: false,
            },
          ]}
        />
      </main>

      <footer className="fixed bottom-4 right-4 pointer-events-none">
        <Logo />
      </footer>
    </>
  );
}

export default App;
