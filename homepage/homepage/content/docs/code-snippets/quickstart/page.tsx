import { Festival } from "@/app/components/Festival";
import { NewBand } from "@/app/components/NewBand";

export default function Home() {
  return (
    <main>
      <h1>🎪 My Festival</h1>
      <NewBand />
      <Festival />
    </main>
  );
}
