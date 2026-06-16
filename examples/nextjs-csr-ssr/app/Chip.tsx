export type RenderedOn = "server" | "hydrated" | "client";

const styles: Record<RenderedOn, string> = {
  server: "border-emerald-500/40 bg-emerald-500/10 text-emerald-600",
  hydrated: "border-sky-500/40 bg-sky-500/10 text-sky-600",
  client: "border-violet-500/40 bg-violet-500/10 text-violet-600",
};

export function Chip({ renderedOn }: { renderedOn: RenderedOn }) {
  return (
    <span
      className={`mb-4 inline-block rounded-full border px-2 py-0.5 font-mono text-[10px] uppercase tracking-widest ${styles[renderedOn]}`}
    >
      rendered: {renderedOn}
    </span>
  );
}
