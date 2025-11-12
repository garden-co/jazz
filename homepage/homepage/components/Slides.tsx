"use client";
import { createContext, useContext, useEffect, useState } from "react";
import { co, z } from "jazz-tools";
import { JazzReactProvider, useCoState } from "jazz-tools/react";

const SlideProgress = co.map({
  slide: z.number(),
});

export function Slides({ children }: { children: React.ReactNode[] }) {
  return (
    <JazzReactProvider sync={{ peer: "wss://cloud.jazz.tools" }}>
      <SlidesInner>{children}</SlidesInner>
    </JazzReactProvider>
  );
}

export function SlidesInner({ children }: { children: React.ReactNode[] }) {
  const [progressId, setProgressId] = useState(window?.location.hash.slice(1));

  const [notesOnly, setNotesOnly] = useState(false);

  useEffect(() => {
    if (window.innerWidth < window.innerHeight) {
      setNotesOnly(true);
    }
  }, []);

  useEffect(() => {
    if (!progressId) {
      const group = co.group().create();
      group.makePublic("writer");
      const progress = SlideProgress.create(
        {
          slide: 0,
        },
        group,
      );
      setProgressId(progress.$jazz.id);
      window.location.hash = progress.$jazz.id;
    }
  }, [progressId]);

  const progress = useCoState(SlideProgress, progressId);

  // use arrow keys to navigate through the slides
  useEffect(() => {
    if (!progress.$isLoaded) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      const s = progress.slide;
      if (event.key === "ArrowRight") {
        progress.$jazz.set("slide", (s + 1) % children.length);
      } else if (event.key === "ArrowLeft") {
        progress.$jazz.set(
          "slide",
          (s - 1 + children.length) % children.length,
        );
      }
    };

    const handlePointerDown = (event: PointerEvent) => {
      if (event.clientX > window.innerWidth / 4) {
        progress.$jazz.set("slide", (progress.slide + 1) % children.length);
      } else {
        progress.$jazz.set(
          "slide",
          (progress.slide - 1 + children.length) % children.length,
        );
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    document.addEventListener("pointerdown", handlePointerDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.removeEventListener("pointerdown", handlePointerDown);
    };
  }, [progress.$isLoaded]);

  console.log(progress.$isLoaded && progress.slide);

  return (
    <NotesOnlyContext.Provider value={notesOnly}>
      <div className="slides">
        {children.map((child, index) =>
          progress.$isLoaded && progress.slide === index ? child : null,
        )}
      </div>
    </NotesOnlyContext.Provider>
  );
}

const NotesOnlyContext = createContext(false);

export function Slide({
  notes,
  children,
}: {
  notes: string[];
  children: React.ReactNode;
}) {
  const notesOnly = useContext(NotesOnlyContext);
  return (
    <div className="relative flex h-screen w-screen flex-col justify-center gap-5 p-20">
      {!notesOnly && children}
      {notesOnly && (
        <div className="absolute left-1 right-1 top-2 z-10 rounded-lg bg-black/90 p-4 text-3xl text-white flex flex-col gap-5">
          {notes.map((note, index) => (
            <div key={index}>{note}</div>
          ))}
        </div>
      )}
    </div>
  );
}
