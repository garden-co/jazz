"use client";

import { createContext, useContext, type ReactNode } from "react";

const PresentationSlideContext = createContext<string | null>(null);
const PresentationRenderModeContext = createContext<"slide" | "notes">("slide");
const PresentationNotesContext = createContext<{
  cumulativeDurationSeconds: number;
  currentDurationSeconds: number;
  slideCount: number;
  slideNumber: number;
  slideTitle: string;
} | null>(null);

type PresentationDeckViewProps = {
  activeSlide: string;
  children: ReactNode;
  mode?: "slide" | "notes";
};

type SlideProps = {
  children: ReactNode;
  slug: string;
  title?: string;
};

type PresentationNotesProviderProps = {
  children: ReactNode;
  cumulativeDurationSeconds: number;
  currentDurationSeconds: number;
  slideCount: number;
  slideNumber: number;
  slideTitle: string;
};

export function PresentationDeckView({
  activeSlide,
  children,
  mode = "slide",
}: PresentationDeckViewProps) {
  return (
    <PresentationSlideContext.Provider value={activeSlide}>
      <PresentationRenderModeContext.Provider value={mode}>
        {children}
      </PresentationRenderModeContext.Provider>
    </PresentationSlideContext.Provider>
  );
}

export function PresentationNotesProvider({
  children,
  cumulativeDurationSeconds,
  currentDurationSeconds,
  slideCount,
  slideNumber,
  slideTitle,
}: PresentationNotesProviderProps) {
  return (
    <PresentationNotesContext.Provider
      value={{
        cumulativeDurationSeconds,
        currentDurationSeconds,
        slideCount,
        slideNumber,
        slideTitle,
      }}
    >
      {children}
    </PresentationNotesContext.Provider>
  );
}

export function Slide({ children, slug }: SlideProps) {
  const activeSlide = useContext(PresentationSlideContext);
  const renderMode = useContext(PresentationRenderModeContext);

  if (activeSlide && activeSlide !== slug) return null;

  if (renderMode === "notes") {
    return <div className="min-h-full [&>*:not([data-presentation-notes])]:hidden">{children}</div>;
  }

  return (
    <div className="flex min-h-full flex-col justify-center gap-6 [&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&>h1]:max-w-[12ch] [&>h1]:text-balance [&>h1]:text-[clamp(3.5rem,8vw,7.5rem)] [&>h1]:font-black [&>h1]:leading-[0.9] [&>h1]:tracking-[-0.05em] [&>h2]:max-w-[20ch] [&>h2]:text-balance [&>h2]:text-[clamp(1.75rem,3vw,3rem)] [&>h2]:font-extrabold [&>h2]:leading-none [&>h2]:tracking-[-0.04em] [&>h2]:text-fd-muted-foreground [&>p]:max-w-[46rem] [&>p]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>p]:leading-[1.5] [&>p]:text-fd-muted-foreground [&>ul]:grid [&>ul]:gap-[0.85rem] [&>ul]:pl-5 [&>ol]:grid [&>ol]:gap-[0.85rem] [&>ol]:pl-5 [&>ul>li]:max-w-[46rem] [&>ul>li]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>ul>li]:leading-[1.5] [&>ul>li]:text-fd-muted-foreground [&>ol>li]:max-w-[46rem] [&>ol>li]:text-[clamp(1.125rem,1.8vw,1.6rem)] [&>ol>li]:leading-[1.5] [&>ol>li]:text-fd-muted-foreground [&_strong]:text-inherit">
      {children}
    </div>
  );
}

export function Notes({ children }: { children: ReactNode }) {
  const renderMode = useContext(PresentationRenderModeContext);
  const notes = useContext(PresentationNotesContext);

  if (renderMode !== "notes" || !notes) return null;

  return (
    <section
      data-presentation-notes
      className="flex min-h-screen flex-col bg-fd-background text-fd-foreground"
    >
      <header className="border-b border-fd-border/70 px-[4vw] py-[2vw]">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="space-y-1">
            <p className="text-[clamp(0.1.4rem,1.6vw,2rem)] font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Slide {notes.slideNumber} / {notes.slideCount}
            </p>
            <h1 className="max-w-[28ch] text-balance text-[clamp(1.2rem,1.8vw,2.4rem)] font-semibold leading-tight tracking-[-0.03em] text-fd-foreground">
              {notes.slideTitle}
            </h1>
          </div>
          <p className="text-[clamp(0.1.4rem,1.6vw,2rem)] font-medium text-fd-muted-foreground">
            {formatDuration(notes.currentDurationSeconds)} this slide /{" "}
            {formatDuration(notes.cumulativeDurationSeconds)} total
          </p>
        </div>
      </header>

      <main className="flex-1 px-[4vw] py-[3vw]">
        <div className="max-w-none text-black [font-size:clamp(1.5rem,2.4vw,3.6rem)] leading-[1.45] [&>*+*]:mt-[1.2em] [&_li]:ml-[1.2em] [&_li]:pl-[0.3em] [&_ol]:grid [&_ol]:gap-[0.6em] [&_p]:max-w-none [&_ul]:grid [&_ul]:gap-[0.6em]">
          {children}
        </div>
      </main>
    </section>
  );
}

function formatDuration(totalSeconds: number) {
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
