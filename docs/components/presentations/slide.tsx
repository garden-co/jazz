"use client";

import { cn } from "@/lib/cn";
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
  className?: string;
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

export function Slide({ children, className, slug }: SlideProps) {
  const activeSlide = useContext(PresentationSlideContext);
  const renderMode = useContext(PresentationRenderModeContext);
  const slideClassName = [
    "flex",
    "h-screen",
    "relative",
    "p-[5vw]",
    "flex-col",
    "justify-end",
    "gap-6",
    "[&_*:first-child]:mt-0",
    "[&_*:last-child]:mb-0",
    "[&_h1]:max-w-[15ch]",
    "[&_h1]:text-balance",
    "[&_h1]:text-[12vw]",
    "[&_h1]:font-black",
    "[&_h1]:leading-[0.9]",
    "[&_h1]:tracking-[-0.04em]",
    "[&_h1]:[hanging-punctuation:first]",
    "[&_h2]:max-w-[40ch]",
    "[&_h2]:text-balance",
    "[&_h2]:text-[8vw]",
    "[&_h2]:font-extrabold",
    "[&_h2]:leading-[0.9]",
    "[&_h2]:tracking-[-0.03em]",
    "[&_p]:max-w-[40ch]",
    "[&_p]:text-[2.5vw]",
    "[&_p]:leading-[1.5]",
    "[&_ul]:grid",
    "[&_ul]:gap-[0.85rem]",
    "[&_ol]:grid",
    "[&_ol]:gap-[0.85rem]",
    "[&_ul>li]:max-w-[40ch]",
    "[&_ul>li]:text-[2.5vw]",
    "[&_ul>li]:leading-[1.5]",
    "[&_ol>li]:max-w-[40ch]",
    "[&_ol>li]:text-[2.5vw]",
    "[&_ol>li]:leading-[1.5]",
    "[&_strong]:text-inherit",
  ].join(" ");

  if (activeSlide && activeSlide !== slug) return null;

  if (renderMode === "notes") {
    return <div className="min-h-full [&>*:not([data-presentation-notes])]:hidden">{children}</div>;
  }

  return (
    <>
      <style jsx>{`
        .presentation-slide :global(ul) {
          list-style: none;
          padding-left: 0;
        }
        
        .presentation-slide :global(ol) {
          counter-reset: slide-item;
          list-style: none;
          padding-left: 0;
        }
        
        .presentation-slide :global(ol > li) {
          counter-increment: slide-item;
          padding-left: 2.2em;
          position: relative;
        }
        
        .presentation-slide :global(ol > li::before) {
          content: counter(slide-item) ".";
          left: 0;
          font-variant-numeric: tabular-nums;
          font-feature-settings: "tnum";
          min-width: 1.9em;
          position: absolute;
          text-align: right;
          top: 0;
        }
        
        .presentation-slide :global(ul > li) {
          padding-left: 1.4em;
          position: relative;
        }
        
        .presentation-slide :global(ul > li::before) {
          content: "—";
          left: 0;
          position: absolute;
          top: 0;
        }
      `}</style>
      <div className={cn("presentation-slide", slideClassName, className)}>{children}</div>
    </>
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
