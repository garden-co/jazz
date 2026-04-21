"use client";

import { createContext, useContext, type ReactNode } from "react";

const PresentationSlideContext = createContext<string | null>(null);
const PresentationNotesContext = createContext<{
  cumulativeDurationSeconds: number;
  currentDurationSeconds: number;
  hideNotes: () => void;
  showNotes: boolean;
} | null>(null);

type PresentationDeckViewProps = {
  activeSlide: string;
  children: ReactNode;
};

type SlideProps = {
  children: ReactNode;
  slug: string;
  title?: string;
};

type PresentationNotesProviderProps = {
  cumulativeDurationSeconds: number;
  currentDurationSeconds: number;
  children: ReactNode;
  hideNotes: () => void;
  showNotes: boolean;
};

export function PresentationDeckView({ activeSlide, children }: PresentationDeckViewProps) {
  return (
    <PresentationSlideContext.Provider value={activeSlide}>
      {children}
    </PresentationSlideContext.Provider>
  );
}

export function PresentationNotesProvider({
  cumulativeDurationSeconds,
  currentDurationSeconds,
  children,
  hideNotes,
  showNotes,
}: PresentationNotesProviderProps) {
  return (
    <PresentationNotesContext.Provider
      value={{ cumulativeDurationSeconds, currentDurationSeconds, hideNotes, showNotes }}
    >
      {children}
    </PresentationNotesContext.Provider>
  );
}

export function Slide({ children, slug }: SlideProps) {
  const activeSlide = useContext(PresentationSlideContext);

  if (activeSlide && activeSlide !== slug) return null;

  return (
    <div className="flex min-h-full flex-col justify-center gap-6 [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&>h1]:max-w-[12ch] [&>h1]:text-balance [&>h1]:text-[clamp(3.5rem,8vw,7.5rem)] [&>h1]:font-black [&>h1]:leading-[0.9] [&>h1]:tracking-[-0.05em] [&>h2]:max-w-[20ch] [&>h2]:text-balance [&>h2]:text-[clamp(1.75rem,3vw,3rem)] [&>h2]:font-extrabold [&>h2]:leading-none [&>h2]:tracking-[-0.04em] [&>h2]:text-fd-muted-foreground [&>p]:max-w-[46rem] [&>p]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>p]:leading-[1.5] [&>p]:text-fd-muted-foreground [&>ul]:grid [&>ul]:gap-[0.85rem] [&>ul]:pl-5 [&>ol]:grid [&>ol]:gap-[0.85rem] [&>ol]:pl-5 [&>ul>li]:max-w-[46rem] [&>ul>li]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>ul>li]:leading-[1.5] [&>ul>li]:text-fd-muted-foreground [&>ol>li]:max-w-[46rem] [&>ol>li]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>ol>li]:leading-[1.5] [&>ol>li]:text-fd-muted-foreground [&_strong]:text-inherit">
      {children}
    </div>
  );
}

export function Notes({ children }: { children: ReactNode }) {
  const notes = useContext(PresentationNotesContext);

  if (!notes?.showNotes) return null;

  return (
    <aside className="fixed bottom-4 right-4 z-20 w-[min(24rem,calc(100vw-2rem))] rounded-2xl border border-fd-border/80 bg-fd-background/95 p-5 shadow-2xl backdrop-blur sm:bottom-6 sm:right-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <p className="text-[0.72rem] leading-5 text-fd-muted-foreground">
            {formatDuration(notes.currentDurationSeconds)} this slide /{" "}
            {formatDuration(notes.cumulativeDurationSeconds)} total
          </p>
        </div>
        <button
          type="button"
          onClick={notes.hideNotes}
          className="text-xs font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
        >
          Hide
        </button>
      </div>
      <div className="mt-4 text-md leading-[1.5] text-fd-muted-foreground [&>*+*]:mt-4 [&_p]:max-w-none [&_li]:max-w-none [&_ul]:grid [&_ul]:gap-[0.55rem] [&_ul]:pl-4 [&_ol]:grid [&_ol]:gap-[0.55rem] [&_ol]:pl-4">
        {children}
      </div>
    </aside>
  );
}

function formatDuration(totalSeconds: number) {
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
