import { Home } from "./components/Home.tsx";

export default function App() {
  return (
    <div className="h-full max-w-xl mx-auto px-4 flex flex-col gap-4 justify-center min-h-screen">
      <h1 className="mx-auto pt-8 flex flex-col gap-6 text-lg">
        Jazz Cloud Auth (React)
      </h1>
      <Home />
    </div>
  );
}
