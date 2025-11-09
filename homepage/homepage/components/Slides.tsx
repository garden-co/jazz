"use client";
import { useEffect, useState } from "react";

export function Slides({ children }: { children: React.ReactNode[] }) {
  const [currentSlide, setCurrentSlide] = useState<number | "all">(
    typeof window === "undefined" ||
    window?.location.hash.slice(1) === ""
      ? 0
      : parseInt(window?.location.hash.slice(1)),
  );

  // use arrow keys to navigate through the slides
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "ArrowRight") {
        setCurrentSlide((s) => {
          const newSlide = s === "all" ? 0 : (s + 1) % children.length;
          window.location.hash = `#${newSlide}`;
          return newSlide;
        });
      } else if (event.key === "ArrowLeft") {
        setCurrentSlide((s) => {
          const newSlide =
            s === "all"
              ? children.length - 1
              : (s - 1 + children.length) % children.length;
          window.location.hash = `#${newSlide}`;
          return newSlide;
        });
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [currentSlide]);

  return (
    <div className="slides">
      {children.map((child, index) =>
        currentSlide === "all" || currentSlide === index ? child : null,
      )}
    </div>
  );
}
