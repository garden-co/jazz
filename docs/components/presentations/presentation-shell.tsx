"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  startTransition,
  useEffect,
  useEffectEvent,
  useRef,
  useState,
  type ReactNode,
} from "react";

type SlideLink = {
  estimatedDurationSeconds: number;
  href: string;
  notesHref: string;
  title: string;
};

type PresentationShellProps = {
  children: ReactNode;
  deckTitle: string;
  slides: SlideLink[];
};

const NOTES_WINDOW_NAME = "jazz-presentation-notes";
const NOTES_WINDOW_FEATURES =
  "popup=yes,width=960,height=1080,left=80,top=80,resizable=yes,scrollbars=yes";

export function PresentationShell({ children, deckTitle, slides }: PresentationShellProps) {
  const pathname = usePathname();
  const router = useRouter();
  const notesWindowRef = useRef<Window | null>(null);
  const [showNotesWindow, setShowNotesWindow] = useState(false);

  const currentIndex = slides.findIndex((slide) => slide.href === pathname);
  const activeIndex = currentIndex === -1 ? 0 : currentIndex;
  const currentSlide = slides[activeIndex] ?? slides[0];
  const previousSlide = slides[activeIndex - 1];
  const nextSlide = slides[activeIndex + 1];

  const syncNotesWindow = useEffectEvent((notesHref: string, focus = false) => {
    const notesWindow = notesWindowRef.current;

    if (!notesWindow || notesWindow.closed) {
      notesWindowRef.current = null;
      setShowNotesWindow(false);
      return;
    }

    if (notesWindow.location.pathname !== notesHref) {
      notesWindow.location.replace(notesHref);
    }

    if (focus) {
      notesWindow.focus();
    }
  });

  const navigateTo = useEffectEvent((index: number) => {
    const target = slides[index];

    if (!target || index === activeIndex) return;

    syncNotesWindow(target.notesHref);

    startTransition(() => {
      router.push(target.href);
    });
  });

  const closeNotesWindow = useEffectEvent(() => {
    const notesWindow = notesWindowRef.current;

    if (notesWindow && !notesWindow.closed) {
      notesWindow.close();
    }

    notesWindowRef.current = null;
    setShowNotesWindow(false);
  });

  const openNotesWindow = useEffectEvent(() => {
    const existingNotesWindow = notesWindowRef.current;

    if (existingNotesWindow && !existingNotesWindow.closed) {
      syncNotesWindow(currentSlide.notesHref, true);
      setShowNotesWindow(true);
      return;
    }

    const notesWindow = window.open(
      currentSlide.notesHref,
      NOTES_WINDOW_NAME,
      NOTES_WINDOW_FEATURES,
    );

    if (!notesWindow) {
      setShowNotesWindow(false);
      return;
    }

    notesWindowRef.current = notesWindow;
    notesWindow.focus();
    setShowNotesWindow(true);
  });

  const toggleNotesWindow = useEffectEvent(() => {
    const notesWindow = notesWindowRef.current;

    if (notesWindow && !notesWindow.closed) {
      closeNotesWindow();
      return;
    }

    openNotesWindow();
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
        toggleNotesWindow();
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

  useEffect(() => {
    router.prefetch(currentSlide.notesHref);
    if (previousSlide) {
      router.prefetch(previousSlide.href);
      router.prefetch(previousSlide.notesHref);
    }
    if (nextSlide) {
      router.prefetch(nextSlide.href);
      router.prefetch(nextSlide.notesHref);
    }
  }, [currentSlide.notesHref, nextSlide, previousSlide, router]);

  useEffect(() => {
    if (!showNotesWindow) return;

    syncNotesWindow(currentSlide.notesHref);
  }, [currentSlide.notesHref, showNotesWindow, syncNotesWindow]);

  useEffect(() => {
    if (!showNotesWindow) return;

    const poll = window.setInterval(() => {
      const notesWindow = notesWindowRef.current;

      if (notesWindow && !notesWindow.closed) return;

      notesWindowRef.current = null;
      setShowNotesWindow(false);
    }, 500);

    return () => {
      window.clearInterval(poll);
    };
  }, [showNotesWindow]);

  useEffect(() => {
    return () => {
      const notesWindow = notesWindowRef.current;
      if (notesWindow && !notesWindow.closed) {
        notesWindow.close();
      }
    };
  }, []);

  return (
    <div className="min-h-screen bg-fd-background text-fd-foreground">
      <div className="flex min-h-screen flex-col">
        <header className="flex items-center justify-between gap-6 px-6 py-4 text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground sm:px-8">
          <div className="min-w-0">
            <p className="truncate">{deckTitle}</p>
          </div>
          <div className="hidden shrink-0 md:block">
            <p>
              Arrows / Space navigate. <kbd className="font-mono normal-case">S</kbd> toggles
              presenter notes.
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
                  onClick={() => syncNotesWindow(previousSlide.notesHref)}
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
                  onClick={() => syncNotesWindow(nextSlide.notesHref)}
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
      </div>
    </div>
  );
}
