import type { ReactNode } from "react";
import ServerTodo from "./ServerTodo";
import TodoPanel from "./TodoPanel";
import PrefetchedTodoPanel from "./PrefetchedTodoPanel";
import { RenderChip, ClientChip } from "./RenderChip";
import { Chip } from "./Chip";

export const dynamic = "force-dynamic";

function ColumnNote({ children }: { children: ReactNode }) {
  return (
    <details className="mb-4">
      <summary className="cursor-pointer select-none text-xs text-foreground/40 hover:text-foreground/60">
        What is this column doing?
      </summary>
      <p className="mt-2 text-xs text-foreground/50 leading-relaxed">{children}</p>
    </details>
  );
}

export default function Home() {
  return (
    <main className="min-h-screen p-8 max-w-6xl w-full mx-auto">
      <h1 className="text-lg font-semibold mb-6 text-foreground/50 tracking-tight">
        jazz — nextjs CSR / SSR
      </h1>
      <div className="grid grid-cols-3 gap-6">
        <section className="border border-foreground/10 rounded-lg p-5">
          <span className="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
            Client-side (React)
          </span>
          <ClientChip />
          <ColumnNote>
            This column includes a fully client-rendered to-do list. Adding items works locally, and
            they sync to the server in the background.
          </ColumnNote>
          <TodoPanel />
        </section>
        <section className="border border-foreground/10 rounded-lg p-5">
          <span className="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
            Server-side (RSC)
          </span>
          <Chip renderedOn="server" />
          <ColumnNote>
            This column includes a fully server-rendered to-do list. Adding items works on the
            server. After an item is added, the page is revalidated, so the server-rendered list
            re-renders with the new item. This is <em>not</em> a live subscription, so adding an
            item in another column won’t update this list.
          </ColumnNote>
          <ServerTodo />
        </section>
        <section className="border border-foreground/10 rounded-lg p-5">
          <span className="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
            Server prefetch + client hydrate
          </span>
          <RenderChip />
          <ColumnNote>
            This column first renders on the server, so the page loaded by the browser already has
            its items. Once loaded, it hydrates on the client, and further updates sync in real
            time. You will see this column and the client-rendered column update in real time if you
            add items here.
          </ColumnNote>
          <PrefetchedTodoPanel />
        </section>
      </div>
    </main>
  );
}
