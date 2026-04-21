"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { startTransition, useEffect, useEffectEvent, useState, type ReactNode } from "react";

type SlideLink = {
  href: string;
  notes: string[];
  title: string;
};

type PresentationShellProps = {
  children: ReactNode;
  deckTitle: string;
  slides: SlideLink[];
};

export function PresentationShell({ children, deckTitle, slides }: PresentationShellProps) {
  const pathname = usePathname();
  const router = useRouter();
  const [showNotes, setShowNotes] = useState(false);

  const currentIndex = slides.findIndex((slide) => slide.href === pathname);
  const activeIndex = currentIndex === -1 ? 0 : currentIndex;
  const currentSlide = slides[activeIndex] ?? slides[0];
  const notes = currentSlide?.notes ?? [];

  const previousSlide = slides[activeIndex - 1];
  const nextSlide = slides[activeIndex + 1];

  const navigateTo = useEffectEvent((index: number) => {
    const target = slides[index];

    if (!target || index === activeIndex) return;

    startTransition(() => {
      router.push(target.href);
    });
  });

  const toggleNotes = useEffectEvent(() => {
    setShowNotes((current) => !current);
  });

  const onKeyDown = useEffectEvent((event: KeyboardEvent) => {
    if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return;

    const target = event.target;
    if (
      target instanceof HTMLElement &&
      (target.isContentEditable || ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName))
    ) {
      return;
    }

    switch (event.key) {
      case "ArrowRight":
      case "ArrowDown":
      case "PageDown":
      case " ":
        event.preventDefault();
        navigateTo(activeIndex + 1);
        return;
      case "ArrowLeft":
      case "ArrowUp":
      case "PageUp":
      case "Backspace":
        event.preventDefault();
        navigateTo(activeIndex - 1);
        return;
      case "Home":
        event.preventDefault();
        navigateTo(0);
        return;
      case "End":
        event.preventDefault();
        navigateTo(slides.length - 1);
        return;
      case "s":
      case "S":
        event.preventDefault();
        toggleNotes();
        return;
      default:
        return;
    }
  });

  useEffect(() => {
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [onKeyDown]);

  return (
    <div className="min-h-screen bg-fd-background text-fd-foreground">
      <div className="flex min-h-screen flex-col">
        <header className="flex items-center justify-between gap-6 px-6 py-4 text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground sm:px-8">
          <div className="min-w-0">
            <p className="truncate">{deckTitle}</p>
          </div>
          <div className="hidden shrink-0 md:block">
            <p>
              Arrows / Space navigate. <kbd className="font-mono normal-case">S</kbd> toggles notes.
            </p>
          </div>
          <div className="shrink-0">
            <p>
              {activeIndex + 1} / {slides.length}
            </p>
          </div>
        </header>

        <main className="flex flex-1 items-stretch px-4 pb-4 sm:px-6 sm:pb-6 lg:px-8 lg:pb-8">
          <div className="mx-auto flex w-full max-w-[1600px] flex-1 flex-col rounded-[2rem] border border-fd-border/70 bg-fd-card/40 shadow-[0_24px_80px_rgba(0,0,0,0.08)] backdrop-blur">
            <div className="flex-1 overflow-auto px-6 py-8 sm:px-10 sm:py-10 lg:px-16 lg:py-14">
              {children}
            </div>
            <footer className="flex items-center justify-between gap-4 border-t border-fd-border/70 px-6 py-4 sm:px-10 lg:px-16">
              {previousSlide ? (
                <Link
                  href={previousSlide.href}
                  className="text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
                >
                  Previous
                </Link>
              ) : (
                <span className="text-sm text-fd-muted-foreground/60">Beginning</span>
              )}
              {nextSlide ? (
                <Link
                  href={nextSlide.href}
                  className="text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
                >
                  Next
                </Link>
              ) : (
                <span className="text-sm text-fd-muted-foreground/60">End</span>
              )}
            </footer>
          </div>
        </main>

        {showNotes ? (
          <aside className="fixed bottom-4 right-4 z-20 w-[min(24rem,calc(100vw-2rem))] rounded-2xl border border-fd-border/80 bg-fd-background/95 p-5 shadow-2xl backdrop-blur sm:bottom-6 sm:right-6">
            <div className="flex items-center justify-between gap-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Presenter notes
              </p>
              <button
                type="button"
                onClick={() => setShowNotes(false)}
                className="text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
              >
                Hide
              </button>
            </div>
            <div className="mt-4 space-y-3 text-sm leading-relaxed text-fd-muted-foreground">
              {notes.length > 0 ? (
                notes.map((note) => <p key={note}>{note}</p>)
              ) : (
                <p>No notes on this slide yet.</p>
              )}
            </div>
          </aside>
        ) : null}
      </div>
    </div>
  );
}
