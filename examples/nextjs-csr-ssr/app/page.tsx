import ClientTodo from "./ClientTodo";
import ServerTodo from "./ServerTodo";

export default function Home() {
  return (
    <main className="min-h-screen p-8 max-w-5xl mx-auto">
      <h1 className="text-lg font-semibold mb-6 text-foreground/50 tracking-tight">
        jazz — nextjs CSR / SSR
      </h1>
      <div className="grid grid-cols-2 gap-6">
        <section className="border border-foreground/10 rounded-lg p-5">
          <span className="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
            Client-side (React)
          </span>
          <ClientTodo />
        </section>
        <section className="border border-foreground/10 rounded-lg p-5">
          <span className="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
            Server-side (RSC)
          </span>
          <ServerTodo />
        </section>
      </div>
    </main>
  );
}
