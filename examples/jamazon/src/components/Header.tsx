import { Link } from "@tanstack/react-router";
import { useDb } from "jazz-tools/react";
import { Music2, ShoppingCart } from "lucide-react";

export default function Header() {
  const db = useDb();

  const handleDeleteStorage = async () => {
    await db.deleteClientStorage();
  };

  return (
    <header className="sticky top-0 z-40 border-b border-amber-800/40 bg-neutral-950/95 backdrop-blur">
      <div className="mx-auto flex w-full max-w-7xl items-center justify-between gap-4 px-4 py-3 md:px-6">
        <Link to="/" className="flex items-center gap-2 text-amber-200 hover:text-amber-100">
          <Music2 className="h-6 w-6" />
          <span className="text-lg font-bold tracking-wide">Jamazon</span>
        </Link>
        <div className="hidden text-sm text-amber-100/80 md:block">
          Instruments with instant local-first checkout
        </div>
        <div className="flex items-center gap-2 rounded-full border border-amber-700/40 px-3 py-1 text-xs text-amber-200">
          <ShoppingCart className="h-4 w-4" />
          Jazz powered
        </div>
        <button onClick={handleDeleteStorage}>Delete storage</button>
      </div>
    </header>
  );
}
