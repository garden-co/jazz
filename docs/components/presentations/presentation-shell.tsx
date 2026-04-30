"use client";

import {
  getPresentationNavigationTargetIndex,
  useActivePresentationSlideIndex,
} from "@/components/presentations/slide";
import { readLetterCanvasArrowNavigationDirection } from "@/lib/presentation-deck";
import { useEffect, useEffectEvent, useRef, useState, type ReactNode } from "react";

type SlideLink = {
  estimatedDurationSeconds: number;
  href: string;
  notesHref: string;
  slug: string;
  title: string;
};

type PresentationShellProps = {
  children: ReactNode;
  deckTitle: string;
  preloadImageSrcs?: string[];
  slides: SlideLink[];
};

const NOTES_WINDOW_NAME = "jazz-presentation-notes";
const NOTES_WINDOW_FEATURES =
  "popup=yes,width=960,height=1080,left=80,top=80,resizable=yes,scrollbars=yes";
const LETTER_CANVAS_IFRAME_ID = "letter-canvas";

export function PresentationShell({
  children,
  deckTitle,
  preloadImageSrcs = [],
  slides,
}: PresentationShellProps) {
  const preloadedImagesRef = useRef<HTMLImageElement[]>([]);
  const notesWindowRef = useRef<Window | null>(null);
  const [showNotesWindow, setShowNotesWindow] = useState(false);

  const activeIndex = useActivePresentationSlideIndex(slides);
  const currentSlide = slides[activeIndex] ?? slides[0];

  const syncNotesWindow = useEffectEvent((notesHref: string, focus = false) => {
    const notesWindow = notesWindowRef.current;

    if (!notesWindow || notesWindow.closed) {
      notesWindowRef.current = null;
      setShowNotesWindow(false);
      return;
    }

    const currentNotesHref = `${notesWindow.location.pathname}${notesWindow.location.hash}`;

    if (currentNotesHref !== notesHref) {
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
    window.location.hash = new URL(target.href, window.location.href).hash;
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
    if (!event.defaultPrevented && !event.metaKey && !event.ctrlKey && !event.altKey) {
      const target = event.target;

      if (
        target instanceof HTMLElement &&
        (target.isContentEditable || ["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName))
      ) {
        return;
      }

      switch (event.key) {
        case "s":
        case "S":
          event.preventDefault();
          toggleNotesWindow();
          return;
      }
    }

    const targetIndex = getPresentationNavigationTargetIndex(event, activeIndex, slides.length);

    if (targetIndex === null) return;

    event.preventDefault();
    navigateTo(targetIndex);
  });

  const onMessage = useEffectEvent((event: MessageEvent<unknown>) => {
    const iframe = document.querySelector<HTMLIFrameElement>(`#${LETTER_CANVAS_IFRAME_ID}`);

    if (!iframe || event.source !== iframe.contentWindow) return;

    const iframeSrc = iframe.getAttribute("src");

    if (!iframeSrc) return;

    const iframeOrigin = new URL(iframeSrc, window.location.href).origin;

    if (event.origin !== iframeOrigin) return;

    const direction = readLetterCanvasArrowNavigationDirection(event.data);

    if (direction === "next") {
      navigateTo(Math.min(activeIndex + 1, slides.length - 1));
    }

    if (direction === "previous") {
      navigateTo(Math.max(activeIndex - 1, 0));
    }
  });

  useEffect(() => {
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [onKeyDown]);

  useEffect(() => {
    preloadedImagesRef.current = preloadImageSrcs.map((src) => {
      const image = new Image();

      image.decoding = "sync";
      image.loading = "eager";
      image.src = src;
      void image.decode?.().catch(() => {
        // The mounted hidden <img> below still keeps the resource warm if decode() is unavailable.
      });

      return image;
    });

    return () => {
      preloadedImagesRef.current = [];
    };
  }, [preloadImageSrcs]);

  useEffect(() => {
    window.addEventListener("message", onMessage);

    return () => {
      window.removeEventListener("message", onMessage);
    };
  }, [onMessage]);

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
    <div
      className="min-h-screen bg-fd-background text-fd-foreground"
      aria-label={`${deckTitle} slide ${activeIndex + 1} of ${slides.length}`}
    >
      <div
        aria-hidden="true"
        className="pointer-events-none fixed left-[-9999px] top-0 h-px w-px overflow-hidden opacity-0"
      >
        {preloadImageSrcs.map((src) => (
          <img key={src} src={src} alt="" decoding="sync" loading="eager" />
        ))}
      </div>
      <main className="w-screen h-screen">{children}</main>
    </div>
  );
}
