"use client";

import { createContext, useContext, type ReactNode } from "react";

const PresentationSlideContext = createContext<string | null>(null);
const PresentationNotesContext = createContext<{
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
  children,
  hideNotes,
  showNotes,
}: PresentationNotesProviderProps) {
  return (
    <PresentationNotesContext.Provider value={{ hideNotes, showNotes }}>
      {children}
    </PresentationNotesContext.Provider>
  );
}

export function Slide({ children, slug }: SlideProps) {
  const activeSlide = useContext(PresentationSlideContext);

  if (activeSlide && activeSlide !== slug) return null;

  return <div className="presentation-slide">{children}</div>;
}

export function Notes({ children }: { children: ReactNode }) {
  const notes = useContext(PresentationNotesContext);

  if (!notes?.showNotes) return null;

  return (
    <aside className="fixed bottom-4 right-4 z-20 w-[min(24rem,calc(100vw-2rem))] rounded-2xl border border-fd-border/80 bg-fd-background/95 p-5 shadow-2xl backdrop-blur sm:bottom-6 sm:right-6">
      <div className="flex items-center justify-between gap-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
          Presenter notes
        </p>
        <button
          type="button"
          onClick={notes.hideNotes}
          className="text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
        >
          Hide
        </button>
      </div>
      <div className="prose mt-4 max-w-none text-sm leading-relaxed text-fd-muted-foreground">
        {children}
      </div>
    </aside>
  );
}
