"use client";
import { useEffect, useState } from "react";

export function Slides({ children }: { children: React.ReactNode[] }) {
  const [currentSlide, setCurrentSlide] = useState<number | "all">(0);

  // use arrow keys to navigate through the slides
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "ArrowRight") {
        setCurrentSlide(s => s === "all" ? 0 : (s + 1) % children.length);
      } else if (event.key === "ArrowLeft") {
        setCurrentSlide(s => s === "all" ? children.length - 1 : (s - 1 + children.length) % children.length);
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [currentSlide]);

  return (
    <div className="slides">
      {children.map((child, index) => (
        currentSlide === "all" || currentSlide === index ? (
          child
        ) : null
      ))}
    </div>
  );
}